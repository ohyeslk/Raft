package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"bytes"
	"encoding/gob"
	"math/rand"
	"io/ioutil"
	"os"
)

const dbg bool = false

// import "bytes"
// import "encoding/gob"
type Role int
const (
	FOLLOWER Role = 1 + iota
	CANDICATE
	LEADER
)

// const for timer
const (
	HEARTBEATINTERVAL int = 60
	HEARTHEATTIMEOUTBASE int = 150
	HEARTBEATTIMEOUTRANGE int = 150
	ELECTIONTIMEOUTBASE int = HEARTHEATTIMEOUTBASE
	ELECTIONTIMEOUTRANGE int = HEARTBEATTIMEOUTRANGE
)

type Entry struct {
	Term 	uint64
	Command interface{}
}

type TermLeader struct {
	Term uint64
	LeaderId int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persist
	currentTerm 	uint64
	votedFor	TermLeader
	log		[]Entry
	startIdx	uint64	// idx that snapshot ends
	startTerm	uint64	// term that snapshot ends


	// mutable
	commitIdx	uint64
	lastApplied	uint64

	// leader only
	nextIdx 	[]uint64
	matchIdx 	[]uint64

	// memory
	applyCh chan ApplyMsg
	heartBeatCh chan *AppendEntriesArgs
	rand 	*rand.Rand
	role 	Role

	// kill signal
	kill	chan bool
	
	// rf.logger.Infomation logger
	logger 	Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.startIdx)
	e.Encode(rf.startTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if len(data) <= 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.startIdx)
	d.Decode(&rf.startTerm)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term		uint64
	CandidateId 	int
	LastLogIdx 	uint64
	LastLogTerm	uint64
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term	uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term 		uint64
	LeaderId 	int
	PrevLogIdx 	uint64
	PrevLogTerm	uint64
	Entries		[]Entry
	LeaderCommit	uint64
	//LeaderStartIdx	uint64
	Snapshot	[]byte
}


type AppendEntriesReply struct {
	Term 	uint64
	MatchedId uint64
	LogSize	int	// for debug
	Success bool
}



func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIdxV := uint64(len(rf.log) - 1)
	lastLogTermV := rf.log[lastLogIdxV].Term
	lastLogIdxV += rf.startIdx

	deny := false

	if rf.currentTerm > args.Term || rf.votedFor.Term > args.Term {
		// candidate's Term is stale
		rf.logger.Trace.Printf("%v denies the vote from %v because stale\n", rf.me, args.CandidateId)
		deny = true
	}else if lastLogTermV > args.LastLogTerm ||
			(lastLogTermV == args.LastLogTerm &&
				lastLogIdxV > args.LastLogIdx){
		// voting server's log is more complete ||
		// (lastTermV > lastTermC) ||
		// (lastTermV == lastTermC) && (lastIndexV > lastIndexC)
		rf.logger.Trace.Printf("%v denies the vote from %v because more complete\n", rf.me, args.CandidateId)
		deny = true
	}else if rf.votedFor.Term == args.Term && rf.votedFor.LeaderId >= 0 {
		// in this Term, voting server has already vote for someone
		rf.logger.Trace.Printf("%v denies the vote from %v because already vote\n", rf.me, args.CandidateId)
		deny = true
	}

	if(deny) {
		// send false ack
		reply.Term = rf.votedFor.Term
		reply.VoteGranted = false
		return
	}

	// otherwise, grant vote
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.votedFor = TermLeader{args.Term, args.CandidateId}

	if rf.commitIdx > args.LastLogIdx {
		rf.logger.Warning.Printf("follower's last term %v lastIdx %v commitIdx %v, requester last term %v, lastIdx %v\n", lastLogTermV, lastLogIdxV, rf.commitIdx, args.LastLogTerm, args.LastLogIdx)
	}

	rf.logger.Trace.Printf("%v term %v vote for %v term %v\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	rf.persist()
	return
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Trace.Printf("%v term %v receive appendEntries from %v term %v, entry len %v, checkIdx %v\n", rf.me, rf.votedFor, args.LeaderId, args.Term, len(args.Entries), args.PrevLogIdx)

	//if args.Term == rf.votedFor.Term && args.LeaderId != rf.votedFor.LeaderId &&
	//	rf.role == FOLLOWER && rf.votedFor.LeaderId != -1 {
	//	// vote for other candidate, who fails the election
	//}

	if rf.currentTerm > args.Term {
		// msg's term is stale
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.logger.Trace.Printf("%v, term %v got a stale term from %v term %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	// treat all messages, whose term >= rf.currentTerm, as a heartBeat
	go func(args *AppendEntriesArgs) {
		rf.heartBeatCh <- args
	}(&args)

	// check the consistency
	logIdxCheck := args.PrevLogIdx
	logTermCheck := args.PrevLogTerm

	if args.Snapshot != nil {
		if logIdxCheck > rf.commitIdx {
			rf.logger.Trace.Printf("server %v receive a snapshot\n", rf.me)
			rf.persister.SaveSnapshot(args.Snapshot)
			rf.startIdx = logIdxCheck
			rf.startTerm = logTermCheck
			rf.applyCh <- ApplyMsg{0, nil, true, args.Snapshot}

			if args.LeaderCommit < logIdxCheck {
				rf.logger.Error.Fatalln("leader's commitIdx <= its startIdx")
			}

			rf.log = args.Entries
			if len(rf.log) <= 0 {
				rf.logger.Error.Fatalln("append snapshot but empty log entries")
			}
			rf.commitIdx = rf.startIdx
			rf.persist()
		}else {
			// re-send snapshot
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.MatchedId = rf.commitIdx
			rf.logger.Trace.Printf("re-send snapshot to %v, get idx %v, reply %v\n", rf.me, logIdxCheck, rf.commitIdx)
			return
		}
	}else if logIdxCheck < rf.startIdx || logIdxCheck >= uint64(len(rf.log)) + rf.startIdx || logTermCheck != rf.log[logIdxCheck - rf.startIdx].Term {
		// consistency check fails
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.MatchedId = rf.commitIdx
		rf.logger.Trace.Printf("appendEngries in %v check consistency fail", rf.me)
		return
	}else if rf.commitIdx > logIdxCheck {
		// my log is more complete than leader's matching index
		// this could happen when network is unreliable
		rf.logger.Trace.Printf("try to delete committed entry in %v, get %v from %v, here %v, leader %v\n", rf.me, logIdxCheck, args.LeaderId, rf.commitIdx, rf.votedFor.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.MatchedId = rf.commitIdx
		reply.LogSize = len(rf.log) + int(rf.startIdx)

		return;
	}else {
		// pass consistency check
		// delete and append entries safely
		rf.log = rf.log[ : logIdxCheck + 1 - rf.startIdx]
		rf.log = append(rf.log, args.Entries...)
	}

	// commit locally
	for cId := rf.commitIdx + 1; cId <= args.LeaderCommit; cId++ {
		if cId >= uint64(len(rf.log)) + rf.startIdx {
			break
		}
		rf.commitIdx = cId
		rf.logger.Trace.Printf("follower %v commit %v %+v", rf.me, cId, rf.log[cId - rf.startIdx])
		rf.applyCh <- ApplyMsg{int(cId), rf.log[cId - rf.startIdx].Command, false, nil}
		rf.persist()
	}
	reply.MatchedId = rf.startIdx + uint64(len(rf.log)) - 1
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}


func (rf *Raft) DeleteOldEntries (commitIdx int, expectedSize int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	oldSize := len(rf.log)

	if commitIdx <= int(rf.startIdx) || expectedSize > rf.persister.RaftStateSize() {
		// already deleted, ignore
		rf.logger.Trace.Printf("server %v already snapshotted, ignore\n", rf.me)
		return
	}

	// update info
	rf.startTerm = rf.log[commitIdx - int(rf.startIdx)].Term
	rf.log = rf.log[commitIdx - int(rf.startIdx) : ]	// delete old log
	rf.startIdx = uint64(commitIdx)

	// persist the updated info
	rf.persist()

	rf.logger.Trace.Printf("server %v delete old entries, start idx is %v, logSize old %v new %v", rf.me, rf.startIdx, oldSize,len(rf.log))
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// log.Println("send", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return -1, -1, false
	}

	//for idx, entry := range rf.log {
	//	if entry.Command == command {
	//		return idx, int(entry.Term), true
	//	}
	//}

	index := len(rf.log)
	Term := rf.currentTerm

	rf.log = append(rf.log, Entry{Term, command})

	rf.matchIdx[rf.me] = uint64(len(rf.log)) - 1
	rf.nextIdx[rf.me] = uint64(len(rf.log))

	rf.logger.Trace.Printf("start a new cmd in server %v term %v\n", rf.me, rf.currentTerm)
	rf.persist()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.sync(i)
	}

	rf.logger.Trace.Printf("new entry %v start in leader %v, index %v, term %v, log size %v\n", command, rf.me, index, Term, len(rf.log))
	return index + int(rf.startIdx), int(Term), true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// rf.logger.Warning.Println("kill")
	rf.kill<-true
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !dbg {
		rf.logger.InitLogger(ioutil.Discard, os.Stdout, os.Stderr, os.Stderr)
	}else {
		rf.logger.InitLogger(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
	}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialization code here.
	rf.currentTerm = 0
	rf.votedFor = TermLeader{0, -1}
	rf.log = make([]Entry, 0)
	rf.startIdx = 0
	rf.startTerm = 0

	// insert a fake entry in the first log
	rf.log = append(rf.log, Entry{0, nil})

	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan *AppendEntriesArgs, 1)
	rf.rand = rand.New(rand.NewSource(int64(rf.me)))
	rf.role = FOLLOWER

	// init server only elements
	rf.nextIdx = make([]uint64, len(rf.peers), len(rf.peers))
	rf.matchIdx = make([]uint64, len(rf.peers), len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIdx = rf.startIdx

	// init kill signal
	rf.kill = make(chan bool, 1)

	rf.logger.Info.Printf("new server %v is up, log size %v, commitId %v\n", me, len(rf.log), rf.commitIdx)
	// begin from follower, expect to receive heartbeat
	go rf.heartBeatTimer()
	return rf
}
