package submission

// This file is the only file you need to modify.
// Add your STM implementation here.

import (
	"assign2/utils"
	"assign2/wg"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
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
	wg     *wg.WaitGroup
	actors []*Actor
	clockChan chan int64
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
