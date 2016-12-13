package raft

import (
	"log"
	"time"
)


func(rf *Raft) leaderCommit() {
	if rf.role != LEADER {
		return
	}
	// find the first entry in current term
	minIdx := 0
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == rf.currentTerm {
			minIdx = i
		}else if rf.log[i].Term < rf.currentTerm {
			break
		}else {
			// already lost leadership, but haven't applied this change
			rf.logger.Trace.Printf("get term %v > current term %v, in server %v, is leader %v\n", rf.log[i].Term, rf.currentTerm, rf.me, rf.role == LEADER)
			return
		}
	}

	if minIdx == 0 {
		// can't find entry in current term
		// unsafe to commit
		return
	}

	minIdx += int(rf.startIdx)

	// find the safe upper bound
	upperBound := rf.commitIdx
	for minIdx < len(rf.log) + int(rf.startIdx)  {
		replicatedNum := 1
		safe := false

		// loop all peers to check whether this entry is replicated
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if int(rf.matchIdx[i]) >= minIdx {
				// entry minIdx has replicated in server i
				replicatedNum++
				if replicatedNum > len(rf.peers) / 2 {
					// replicated in the majority
					safe = true
					upperBound = uint64(minIdx)
					minIdx++
					break
				}
			}
		}
		if !safe {
			break
		}
	}

	cId := rf.commitIdx + 1
	rf.logger.Trace.Printf("leader %v upperbound %v min %v\n", rf.me, upperBound, cId)
	for cId <= upperBound {
		if cId >= uint64(len(rf.log)) + rf.startIdx {
			rf.logger.Error.Fatalln("out of bound")
		}
		rf.logger.Trace.Printf("leader %v commit %v %v", rf.me, cId, rf.log[cId - rf.startIdx])
		rf.applyCh <- ApplyMsg{int(cId), rf.log[cId - rf.startIdx].Command, false, nil}
		rf.commitIdx = cId
		rf.persist()
		cId++
	}

}


func(rf *Raft) sync(server int) (bool, uint64) {
	rf.mu.Lock()

	if rf.role != LEADER {
		rf.mu.Unlock()
		return false, 0
	}

	var matchedLogIdx uint64
	var matchedTerm uint64
	var entries []Entry = nil

	var snapshot []byte = nil
	if rf.nextIdx[server] - 1 < rf.startIdx {
		// a slow follower, send snapshot
		rf.logger.Trace.Printf("server %v is a slow server, send snapshot and whole log to it\n", server)
		matchedLogIdx = rf.startIdx
		matchedTerm = rf.startTerm
		snapshot = rf.persister.ReadSnapshot()
		entries = rf.log

		if len(entries) == 0 {
			rf.logger.Error.Fatalln("0 log")
		}
	}else if rf.nextIdx[server] - 1 == rf.startIdx {
		rf.logger.Trace.Printf("server %v matched to startIdx %v\n", server, rf.nextIdx[server] - 1)
		matchedLogIdx = rf.startIdx
		matchedTerm = rf.startTerm

		entries = rf.log[matchedLogIdx - rf.startIdx + 1: ]
	}else if rf.nextIdx[server] == rf.matchIdx[server] + 1 {
		// consistent
		matchedLogIdx = rf.matchIdx[server]

		if matchedLogIdx + 1 <= uint64(len(rf.log)) + rf.startIdx{
			entries = rf.log[matchedLogIdx - rf.startIdx + 1 : ]
		}else if matchedLogIdx == uint64(len(rf.log)) + rf.startIdx {
			rf.logger.Warning.Printf("%v matched to %v, log len in master %v %v\n", server, matchedLogIdx, rf.me, len(rf.log))
		}
		matchedTerm = rf.log[matchedLogIdx - rf.startIdx].Term
	}else {
		// haven't achieve consistency, but follower is up-to-date
		matchedLogIdx = rf.nextIdx[server] - 1

		entries = rf.log[matchedLogIdx - rf.startIdx + 1 : ]
		matchedTerm = rf.log[matchedLogIdx - rf.startIdx].Term
	}
	rf.mu.Unlock()

	args := AppendEntriesArgs{rf.currentTerm, rf.me, matchedLogIdx, matchedTerm, entries, rf.commitIdx, snapshot}
	rf.logger.Trace.Printf("leader %v to %v send %+v \n", rf.me, server, args)
	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(server, args, reply)

	if !ok {

		return false, 0
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return false, 0
	}

	if reply.Term == rf.currentTerm {
		if reply.MatchedId >= uint64(len(rf.log)) + rf.startIdx {
			rf.logger.Error.Fatalf("follower %v commit %v more than leader %v's logsize %v, commit %v, term %v, follower size %v\n", server, reply.MatchedId, rf.me, uint64(len(rf.log)) + rf.startIdx, rf.commitIdx, rf.currentTerm, reply.LogSize)
		}
		rf.matchIdx[server] = reply.MatchedId
		rf.nextIdx[server] = reply.MatchedId + 1

		if rf.matchIdx[server] == uint64(len(rf.log)) + rf.startIdx {
			rf.logger.Warning.Printf("%v matched to %v, log len in master %v log len %v, commit %v, term %v follower size %v\n", server, rf.matchIdx[server], rf.me, len(rf.log), rf.commitIdx, rf.currentTerm, reply.LogSize)
		}
	}

	if reply.Success {
		rf.leaderCommit()
	}

	return true, reply.Term
}

// used by leader to send out heartbeat
func (rf *Raft) broadcastHeartBeat() {
	waitTime := time.Duration(HEARTBEATINTERVAL)
	timmer := time.NewTimer(waitTime * time.Millisecond)
	for {
		if rf.role != LEADER {
			log.Fatalf("call broadcast heartbeat, but I'm not a leader\n")
		}

		// send out heartheat every HEARTBEATINTERVAL ms

		staleSignal := make(chan bool, len(rf.peers) - 1)

		// broadcast heartheat in parallel
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				// escape myself
				continue
			}
			go func(server int) {
				ok, term := rf.sync(server)
				if ok && term > rf.currentTerm {
					staleSignal <- true
				}
			}(i)
		}

		endLoop := false
		for !endLoop{
			select {
			case <-rf.kill:
				return;
			case <-staleSignal:
			// my Term is stale
			// convert to follower stage
				rf.mu.Lock()
				rf.role = FOLLOWER
				rf.mu.Unlock()
				rf.logger.Trace.Printf("leader %v is stale, turns to follower\n", rf.me)
				go rf.heartBeatTimer()
				return
			case msg := <-rf.heartBeatCh:
			// get a heart beat from others
				if rf.currentTerm == msg.Term {
					// in this Term, there are 2 leaders
					// impossible
					rf.logger.Error.Fatalf("in leader %v's broadcast, receive the same heartbeat Term, value: %v leader: %v\n", rf.me, msg.Term, msg.LeaderId)
				}else if rf.currentTerm < msg.Term {
					// heart beat from a superior leader
					rf.mu.Lock()
					rf.role = FOLLOWER
					rf.currentTerm = msg.Term
					rf.votedFor = TermLeader{msg.Term, msg.LeaderId}
					rf.mu.Unlock()
					rf.logger.Trace.Printf("leader %v finds a superior leader %v, turns to follower\n", rf.me, rf.votedFor)
					go rf.heartBeatTimer()
					return
				}

			case <- timmer.C:
			// begin another broadcast round
				endLoop = true
				timmer.Reset(waitTime * time.Millisecond)
				break
			}
		}
	}
}
