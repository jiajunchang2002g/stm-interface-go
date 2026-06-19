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
	wg        *wg.WaitGroup
	actors    []*Actor
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
			id:       uint32(i),
			reqChan:  make(chan ActorReq),
			val:      0, // init with 0
			version:  0,
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
				reply := make(chan ActorResp)
				readSet, writeSet, earlyAbort := s.executeOptimisticPhase(commands, reply)
				if earlyAbort {
					continue // retry
				}

				actorsToLock := buildActorsToLock(readSet, writeSet)
				acquired, lockedAll := s.lockActors(int64(input.TransactionId), actorsToLock, reply)
				if !lockedAll {
					s.unlockActors(int64(input.TransactionId), acquired, reply)
					continue //Active Retry
				}

				if !s.validateReadSet(readSet, reply) {
					s.unlockActors(int64(input.TransactionId), actorsToLock, reply)
					continue //Active Retry
				}

				s.commitWriteSet(writeSet, reply)
				commitTime := s.GetCurrentTimestamp()

				s.unlockActors(int64(input.TransactionId), actorsToLock, reply)

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

func (s *StmInterface) executeOptimisticPhase(commands []utils.StmCommand, reply chan ActorResp) (map[uint32]uint64, map[uint32]uint32, bool) {
	readSet := make(map[uint32]uint64)
	writeSet := make(map[uint32]uint32)

	for i, cmd := range commands {
		switch cmd.CommandType {
		case utils.READ:
			if val, ok := writeSet[cmd.Address]; ok {
				commands[i].Value = val // Read-your-own-writes
				continue
			}

			s.actors[cmd.Address].reqChan <- ActorReq{Op: READ, ReplyChan: reply}
			res := <-reply
			if oldVer, exists := readSet[cmd.Address]; exists && oldVer != res.Ver {
				return readSet, writeSet, true
			}

			commands[i].Value = res.Val
			readSet[cmd.Address] = res.Ver
		case utils.WRITE:
			writeSet[cmd.Address] = cmd.Value
		}
	}

	return readSet, writeSet, false
}

func buildActorsToLock(readSet map[uint32]uint64, writeSet map[uint32]uint32) []uint32 {
	actorMap := make(map[uint32]bool)
	for addr := range readSet {
		actorMap[addr%NumActors] = true
	}
	for addr := range writeSet {
		actorMap[addr%NumActors] = true
	}

	actorsToLock := make([]uint32, 0, len(actorMap))
	for actorID := range actorMap {
		actorsToLock = append(actorsToLock, actorID)
	}
	sort.Slice(actorsToLock, func(i, j int) bool { return actorsToLock[i] < actorsToLock[j] })
	return actorsToLock
}

func (s *StmInterface) lockActors(txID int64, actorsToLock []uint32, reply chan ActorResp) ([]uint32, bool) {
	acquired := make([]uint32, 0, len(actorsToLock))
	for _, actorID := range actorsToLock {
		s.actors[actorID].reqChan <- ActorReq{Op: LOCK, TxID: txID, ReplyChan: reply}
		res := <-reply
		if !res.Ok {
			return acquired, false
		}
		acquired = append(acquired, actorID)
	}
	return acquired, true
}

func (s *StmInterface) validateReadSet(readSet map[uint32]uint64, reply chan ActorResp) bool {
	for addr, ver := range readSet {
		s.actors[addr].reqChan <- ActorReq{Op: VALIDATE, Ver: ver, ReplyChan: reply}
		res := <-reply
		if !res.Ok {
			return false
		}
	}
	return true
}

func (s *StmInterface) commitWriteSet(writeSet map[uint32]uint32, reply chan ActorResp) {
	for addr, val := range writeSet {
		s.actors[addr].reqChan <- ActorReq{Op: WRITE, Val: val, ReplyChan: reply}
		<-reply
	}
}

func (s *StmInterface) unlockActors(txID int64, actorIDs []uint32, reply chan ActorResp) {
	for _, actorID := range actorIDs {
		s.actors[actorID].reqChan <- ActorReq{Op: UNLOCK, TxID: txID, ReplyChan: reply}
		<-reply
	}
}

func (s *StmInterface) GetCurrentTimestamp() int64 {
	return <-s.clockChan
}
