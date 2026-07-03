package submission

// This file is the only file you need to modify.
// Add your STM implementation here.

import (
	"assign2/utils"
	"assign2/wg"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync/atomic"
)

const NumActors = 256

// HELPER:  Request Types for our Channels
type ReqType int
const (
	READ ReqType = iota
	LOCK
	VALIDATE
	WRITE
	UNLOCK
)

// response / req objects to be sent over channels
type ActorReq struct {
	Op        ReqType
	TxID      int64
	Val       uint32
	Ver       uint64
	ReplyChan chan ActorResp
}

type ActorResp struct {
	Ok  bool
	Val uint32
	Ver uint64
}

// each actor works on one mem loc
type Actor struct {
	id       uint32
	reqChan  chan ActorReq
	val      uint32
	version  uint64
	lockedBy int64 // -1 means unlocked
}

type StmInterface struct {
	wg        *wg.WaitGroup
	actors    []*Actor
	clockChan chan int64
	nextTxID  int64 // atomically incremented for in-process transactions
}

func (s *StmInterface) Init(ctx context.Context, wg *wg.WaitGroup) {
	s.wg = wg
	s.actors = make([]*Actor, NumActors)
	s.clockChan = make(chan int64)
	
	// initialize clock
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var counter int64 = 0
		for {
			select {
			case <-ctx.Done():
				return
			case s.clockChan <- counter:
				counter++
			}
		}
	}()

	// initalize actors goroutines
	for i := 0; i < NumActors; i++ {
		s.actors[i] = &Actor{
			id: uint32(i),
			reqChan: make(chan ActorReq),
			val: 0, // init with 0
			version: 0,
			lockedBy: -1,
		}
		
		s.wg.Add(1)
		go s.runActor(ctx, s.actors[i])
	}
}

// goroutine for each actor
func (s *StmInterface) runActor(ctx context.Context, actor *Actor) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		// process actor requests (for that mem loc)	
		case req := <-actor.reqChan:
			switch req.Op {
			case READ:
				req.ReplyChan <- ActorResp{Ok: true, Val: actor.val, Ver: actor.version}	
			case LOCK:
				if actor.lockedBy == -1 || actor.lockedBy == req.TxID {
					actor.lockedBy = req.TxID
					req.ReplyChan <- ActorResp{Ok: true}
				} else {
					req.ReplyChan <- ActorResp{Ok: false}
				}
			
			case VALIDATE:
				req.ReplyChan <- ActorResp{Ok: actor.version == req.Ver}	
			case WRITE:
				actor.val = req.Val
				actor.version++
				req.ReplyChan <- ActorResp{Ok: true}
			
			case UNLOCK:
				if actor.lockedBy == req.TxID {
					actor.lockedBy = -1
					req.ReplyChan <- ActorResp{Ok: true}
				} else {
				
					req.ReplyChan <- ActorResp{Ok: false}
				}
			}
		}
	}
}

func (s *StmInterface) Shutdown(ctx context.Context) {
	s.wg.Wait()
}

func (s *StmInterface) Accept(ctx context.Context, conn net.Conn) {
	s.wg.Add(2)

	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		conn.Close()
	}()

	go func() {
		defer s.wg.Done()
		s.handleConn(conn)
	}()
}

func (s *StmInterface) handleConn(conn net.Conn) {
	defer conn.Close()
	var commands []utils.StmCommand

	for {
		input, err := utils.ReadInput(conn)
		if err != nil {
			if err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			return
		}

		switch input.CommandType {
		case utils.START:
			commands = nil // Reset history for new transaction

		case utils.READ, utils.WRITE:
			commands = append(commands, input)

		case utils.COMMIT:
			
			// --- LEVEL 3.3.4 OPTIMISTIC CONCURRENCY RETRY LOOP ---
			for {
				readSet := make(map[uint32]uint64)
				writeSet := make(map[uint32]uint32)
			//	localReadCache := make(map[uint32]uint32) // FIX 1: Ensures repeatable reads
				reply := make(chan ActorResp)

				// Phase 1: Local Optimistic Execution
				earlyAbort := false
				for i, cmd := range commands {
					if cmd.CommandType == utils.READ {
						if val, ok := writeSet[cmd.Address]; ok {
							commands[i].Value = val // Read-your-own-writes
			//			} else if val, ok := localReadCache[cmd.Address]; ok {
			//				commands[i].Value = val // FIX 1: Read-your-own-reads
						} else {
							s.actors[cmd.Address].reqChan <- ActorReq{Op: READ, ReplyChan: reply}
							res := <-reply
							
							// If we have read this address before during this transaction...
							if oldVer, exists := readSet[cmd.Address]; exists {
								if oldVer != res.Ver {
									earlyAbort = true
									break 
								}
							}
							
							commands[i].Value = res.Val
							readSet[cmd.Address] = res.Ver
							
							//	s.actors[cmd.Address].reqChan <- ActorReq{Op: READ, ReplyChan: reply}
						//	res := <-reply
						//	commands[i].Value = res.Val
						//	readSet[cmd.Address] = res.Ver
						//	localReadCache[cmd.Address] = res.Val
						}
					} else if cmd.CommandType == utils.WRITE {
						writeSet[cmd.Address] = cmd.Value
					}
				}

				// if change detected early, retry
				if earlyAbort {
					continue // retry
				}

				// Phase 2: Identify and Sort Shards (Prevents Deadlocks)
				actorMap := make(map[uint32]bool)
				for addr := range readSet { actorMap[addr%NumActors] = true }
				for addr := range writeSet { actorMap[addr%NumActors] = true }
				
				var actorsToLock []uint32
				for actorID := range actorMap { actorsToLock = append(actorsToLock, actorID) }
				sort.Slice(actorsToLock, func(i, j int) bool { return actorsToLock[i] < actorsToLock[j] })

				// Phase 3: Lock Shards
				lockedAll := true
				var acquired []uint32
				for _, actorID := range actorsToLock {
					s.actors[actorID].reqChan <- ActorReq{Op: LOCK, TxID: int64(input.TransactionId), ReplyChan: reply}
					res := <-reply
					if !res.Ok {
						lockedAll = false
						break
					}
					acquired = append(acquired, actorID)
				}

				if !lockedAll {
					for _, actorID := range acquired {
						s.actors[actorID].reqChan <- ActorReq{Op: UNLOCK, TxID: int64(input.TransactionId), ReplyChan: reply}
						<-reply
					}
					continue //Active Retry
				}

				// Phase 4: Validate Reads
				isValid := true
				for addr, ver := range readSet {
					s.actors[addr].reqChan <- ActorReq{Op: VALIDATE, Ver: ver, ReplyChan: reply}
					res := <-reply
					if !res.Ok {
						isValid = false
						break
					}
				}

				// If validation failed, unlock and RETRY
				if !isValid {
					for _, actorID := range actorsToLock {
						s.actors[actorID].reqChan <- ActorReq{Op: UNLOCK, TxID: int64(input.TransactionId), ReplyChan: reply}
						<-reply
					}
					continue //Active Retry
				}

				// Phase 5: Commit Writes
				for addr, val := range writeSet {
					s.actors[addr].reqChan <- ActorReq{Op: WRITE, Val: val, ReplyChan: reply}
					<-reply
				}

				commitTime := s.GetCurrentTimestamp()

				// Phase 6: Unlock All
				for _, actorID := range actorsToLock {
					s.actors[actorID].reqChan <- ActorReq{Op: UNLOCK, TxID: int64(input.TransactionId), ReplyChan: reply}
					<-reply
				}

				// transaction commited, print and exit retry loop
				utils.PrintOutput(input.TransactionId, commands, commitTime)
				break 
			}

			commands = nil	
		default:
			// should not reach here
		}
	}
}

func (s *StmInterface) GetCurrentTimestamp() int64 {
	return <-s.clockChan
}

// ErrConflict is returned by Tx.Commit when validation fails due to a
// concurrent modification.
var ErrConflict = errors.New("stm: transaction conflict")

// Tx is an in-process STM transaction created by StmInterface.Begin.
// It is not safe to use concurrently from multiple goroutines.
type Tx struct {
	stm      *StmInterface
	txID     int64
	readSet  map[uint32]uint64 // addr → version at time of read
	writeSet map[uint32]uint32 // addr → value to write
	readVals map[uint32]uint32 // addr → value seen on read
}

// Begin creates a new in-process transaction with a unique auto-generated ID.
func (s *StmInterface) Begin() *Tx {
	txID := atomic.AddInt64(&s.nextTxID, 1)
	return &Tx{
		stm:      s,
		txID:     txID,
		readSet:  make(map[uint32]uint64),
		writeSet: make(map[uint32]uint32),
		readVals: make(map[uint32]uint32),
	}
}

// Read returns the current value at addr.
// Write-your-own-writes and repeatable-reads are honoured: if addr has already
// been written or read in this transaction the cached value is returned.
func (t *Tx) Read(addr uint32) uint32 {
	if val, ok := t.writeSet[addr]; ok {
		return val
	}
	if val, ok := t.readVals[addr]; ok {
		return val
	}
	reply := make(chan ActorResp, 1)
	t.stm.actors[addr].reqChan <- ActorReq{Op: READ, ReplyChan: reply}
	res := <-reply
	t.readSet[addr] = res.Ver
	t.readVals[addr] = res.Val
	return res.Val
}

// Write stages a write of val to addr.  The write is not visible to other
// transactions until Commit succeeds.
func (t *Tx) Write(addr uint32, val uint32) {
	t.writeSet[addr] = val
}

// Commit attempts to commit the transaction using the OCC protocol (phases 3–6).
// It returns the commit timestamp on success, or (−1, ErrConflict) if locking
// or read-set validation fails.  The caller is responsible for retrying with a
// fresh Tx if appropriate.
func (t *Tx) Commit() (int64, error) {
	reply := make(chan ActorResp, 1)

	// Collect all addresses and sort to prevent deadlock.
	addrSet := make(map[uint32]bool)
	for addr := range t.readSet {
		addrSet[addr] = true
	}
	for addr := range t.writeSet {
		addrSet[addr] = true
	}
	addrs := make([]uint32, 0, len(addrSet))
	for addr := range addrSet {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i] < addrs[j] })

	// Phase 3: Lock all addresses.
	var acquired []uint32
	for _, addr := range addrs {
		t.stm.actors[addr].reqChan <- ActorReq{Op: LOCK, TxID: t.txID, ReplyChan: reply}
		if res := <-reply; !res.Ok {
			for _, a := range acquired {
				t.stm.actors[a].reqChan <- ActorReq{Op: UNLOCK, TxID: t.txID, ReplyChan: reply}
				<-reply
			}
			return -1, ErrConflict
		}
		acquired = append(acquired, addr)
	}

	unlockAll := func() {
		for _, addr := range addrs {
			t.stm.actors[addr].reqChan <- ActorReq{Op: UNLOCK, TxID: t.txID, ReplyChan: reply}
			<-reply
		}
	}

	// Phase 4: Validate read-set versions.
	for addr, ver := range t.readSet {
		t.stm.actors[addr].reqChan <- ActorReq{Op: VALIDATE, Ver: ver, ReplyChan: reply}
		if res := <-reply; !res.Ok {
			unlockAll()
			return -1, ErrConflict
		}
	}

	// Phase 5: Apply writes.
	for addr, val := range t.writeSet {
		t.stm.actors[addr].reqChan <- ActorReq{Op: WRITE, Val: val, ReplyChan: reply}
		<-reply
	}

	ts := t.stm.GetCurrentTimestamp()

	// Phase 6: Unlock.
	unlockAll()

	return ts, nil
}
