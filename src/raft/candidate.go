package raft

import (
	"time"
)


// issued a new election Term to become leader, by a candidate
func (rf *Raft) election(electionTerm uint64) {
	// turn into candidate
	// increase current Term
	// vote for myself
	rf.mu.Lock()
	if rf.currentTerm >= electionTerm || (rf.votedFor.Term >= electionTerm && rf.votedFor.LeaderId >= 0) {
		rf.role = FOLLOWER
		if rf.votedFor.Term > rf.currentTerm {
			rf.currentTerm = rf.votedFor.Term
		}
		rf.logger.Trace.Printf("catch a potential race in server %v, election term %v, vote for %+v\n", rf.me, electionTerm, rf.votedFor)
		rf.mu.Unlock()
		go rf.heartBeatTimer()
		return
	}
	rf.role = CANDICATE
	rf.currentTerm = electionTerm
	rf.votedFor = TermLeader{electionTerm, rf.me}
	rf.mu.Unlock()

	rf.logger.Trace.Printf("new election begin in %v, Term %v\n", rf.me, rf.currentTerm)
	lastLogIdx := uint64(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	args := RequestVoteArgs{electionTerm, rf.me, lastLogIdx + rf.startIdx, lastLogTerm}


	type Rec struct {
		ok bool
		reply *RequestVoteReply
	}
	recBuff := make(chan Rec, 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// escape myself
			continue
		}

		// send requestVote in parallel
		go func(server int) {
			reply := new(RequestVoteReply)
			reply.Term = 0
			reply.VoteGranted = false
			ok := rf.sendRequestVote(server, args, reply)
			rf.logger.Trace.Printf("in election %v get reply %v\n", rf.me, reply)
			recBuff <- Rec {ok, reply}
		}(i)
	}

	// signal: wins the election
	winSignal := make(chan bool, 1)
	// signal: my current Term is out of date
	staleSignal := make(chan *RequestVoteReply, 1)
	failSignal := make(chan bool)
	go func(){
		// get an approve from myself
		approveNum := 1
		denyNum := 0
		for i := 0; i < len(rf.peers) - 1; i++{
			rec := <- recBuff
			if !rec.ok {
				continue
			}
			if rec.reply.VoteGranted{
				approveNum++
				if approveNum > len(rf.peers) / 2{
					winSignal <- true
					break
				}
			}else{
				if rec.reply.Term > rf.currentTerm {
					staleSignal <- rec.reply
					break
				}

				denyNum++
				if denyNum > len(rf.peers) / 2 {
					failSignal <- true
					break
				}

			}
		}
	}()

	// election timer
	waitTime := time.Duration(ELECTIONTIMEOUTBASE+ rf.rand.Intn(ELECTIONTIMEOUTRANGE))
	timmer := time.NewTimer(waitTime * time.Millisecond)
	// loop until win, fail, or timeout
	for {
		select {
		case <- rf.kill:
			return
		case msg := <- rf.heartBeatCh:
			if msg.Term < rf.currentTerm {
				// receive stale heartbeat
				// ignore
				break
			}


		// fail the election
		// get heartbeat from other leader
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.role = FOLLOWER
			rf.votedFor = TermLeader{msg.Term, msg.LeaderId}
			rf.mu.Unlock()
			go rf.heartBeatTimer()
			rf.logger.Trace.Printf("candidate %v becomes follower\n", rf.me)
			return
		case <-winSignal:
			rf.mu.Lock()
			// reinit nextIdx, matchIdx
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIdx[i] = uint64(len(rf.log)) + rf.startIdx
				rf.matchIdx[i] = 0
			}
			rf.role = LEADER
			rf.mu.Unlock()
			rf.logger.Info.Printf("candidate %v becomes leader in Term %v, log size %v, commitIdx %v\n", rf.me, rf.currentTerm, uint64(len(rf.log)) + rf.startIdx, rf.commitIdx)
			go rf.broadcastHeartBeat()

			return
		case <- failSignal:
			rf.mu.Lock()
			rf.role = FOLLOWER
			rf.votedFor = TermLeader{rf.currentTerm, -1}
			rf.mu.Unlock()
			rf.persist()
			go rf.heartBeatTimer()
			return

		case reply := <-staleSignal:
			rf.mu.Lock()

		// discover a new Term
		// turn into follower state
		// another kind of failure
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.votedFor = TermLeader{reply.Term, -1}
			rf.mu.Unlock()
			rf.persist()
			go rf.heartBeatTimer()
			return
		case <-timmer.C:
		// fire another election Term
			rf.logger.Trace.Printf("election timeout in candidate %v term %v\n", rf.me, rf.currentTerm)
			go rf.election(electionTerm + 1)
			return
		}
	}

}

