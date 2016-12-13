package raft

import (
	"time"
)


// used by follower
func (rf *Raft) heartBeatTimer() {
	// in the same Term, we use the same timeout
	waitTime := time.Duration(HEARTHEATTIMEOUTBASE + rf.rand.Intn(HEARTBEATTIMEOUTRANGE))
	timmer := time.NewTimer(waitTime * time.Millisecond)
	for {

		if rf.role != FOLLOWER {
			rf.logger.Error.Fatalln("call heartBeatTimer, but I'm not a follower")
		}

		// loop until time out or receive a correct heartbeat
		endLoop := false
		for !endLoop {
			select {
			case <- rf.kill:
				return
			case msg := <-rf.heartBeatCh:
				if rf.currentTerm > msg.Term {
					// stale heart beat
					// ignore and continue the loop
					rf.logger.Trace.Println("%v receive a stale heartbeat", rf.me)
				}else {
					// receive a legal heartbeat
					// break the loop to wait next heartBeat
					rf.mu.Lock()
					rf.currentTerm = msg.Term
					rf.votedFor = TermLeader{msg.Term, msg.LeaderId}
					rf.persist()
					rf.mu.Unlock()
					timmer.Reset(waitTime * time.Millisecond)
					endLoop = true
				}
			case <-timmer.C:
			// time out, end the heartbeat timer
			// and fire a new election Term
				go rf.election(rf.currentTerm + 1)
				return
			}
		}
	}
}

