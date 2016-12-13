package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"os"
	"io/ioutil"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type PendingOps struct {
	Req Op
	Success chan bool
}



type RaftKV struct {
	// persisted data when snapshotting
	data		map[string]string	// kv store data
	stamps		map[int64]int64		// client id --> latest request id

	// in memory
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	persister *raft.Persister
	applyCh chan raft.ApplyMsg
	kill	chan bool

	maxraftstate int // snapshot if log grows this big

	pendingOps	map[int]*PendingOps	// pending requests
	logger	raft.Logger
}

// get a committed msg from Raft
func (kv *RaftKV) receiveApply(msg *raft.ApplyMsg) {

	kv.logger.Trace.Printf("get apply: %+v in server %v\n", msg, kv.me)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.UseSnapshot {
		kv.loadSnapShot(msg.Snapshot)
		return
	}

	idx, req := msg.Index, msg.Command.(Op)

	op, p_ok := kv.pendingOps[idx]
	stamp, s_ok := kv.stamps[req.ClientId]

	if (s_ok && stamp >= req.RequestId) {
		// already execute this cmd, ignore it
		// kv.logger.Warning.Printf("get stale cmd, server's stamp %v, req's stamp %v\n", stamp, req.RequestId)
	} else {
		// haven't execute this request
		kv.logger.Trace.Printf("req type %v\n", req.Type)
		switch req.Type {
		case PUT:
			kv.data[req.Key] = req.Value
		case APPEND:
			kv.data[req.Key] += req.Value
		}
		kv.stamps[req.ClientId] = req.RequestId
	}

	// check state size to make snapshot
	stateSize := kv.raftStateSize()
	if kv.maxraftstate > 0 && stateSize > kv.maxraftstate {
		// exceed threshold, doing snapshot
		kv.logger.Info.Printf("server %v making snapshot\n", kv.me)
		kv.saveSnapShot()
		go func() {
			kv.rf.DeleteOldEntries(idx, stateSize)
		}()
	}

	// check whether need to reply GET op
	if !p_ok {
		return
	}

	// it the request is sent to this server, send back execute result
	if op.Req.RequestId != req.RequestId || op.Req.ClientId != req.ClientId {
		op.Success <- false
	}else {
		if op.Req.Type == GET {
			op.Req.Value = kv.data[op.Req.Key]
		}
		op.Success <- true
	}

	delete(kv.pendingOps, idx)
}

// response to the client request
func (kv *RaftKV) ExecuteRequest(args Op, reply *Reply) {

	// send the request to raft
	idx, _, ok := kv.rf.Start(args)

	if !ok {
		// I'm not leader, reject this reuest
		reply.Success = false
		return;
	}

	kv.logger.Trace.Printf("start %+v in %v, is leader: %v, idx %v\n", args, kv.me, ok, idx)
	// save this request to pending ops
	op := new(PendingOps)
	op.Req = args
	op.Success = make(chan bool, 1)

	kv.mu.Lock()
	if val, ok := kv.pendingOps[idx]; ok {
		val.Success <- false
		kv.logger.Warning.Println("alraedy have a log in this idx")
	}
	kv.pendingOps[idx] = op
	kv.mu.Unlock()

	// whether timing out or executed successfully
	timmer := time.NewTimer(1 * time.Second)
	select {
	case <-timmer.C:
		reply.Success = false
		kv.logger.Trace.Printf("time out for the args %+v\n", args)
		return
	case ok = <- op.Success:
		reply.Success = ok
		if ok && args.Type == GET {
			reply.Value = op.Req.Value
		}
		return
	}
}

func (kv *RaftKV) raftStateSize() int {
	return kv.persister.RaftStateSize()
}

// snapshot current state and return it as a byte array
func (kv *RaftKV) saveSnapShot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.stamps)
	kv.persister.SaveSnapshot(w.Bytes())
}

// load the state from snap shot
func (kv *RaftKV) loadSnapShot(data []byte) {
	if len(data) <= 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.stamps)

	kv.logger.Info.Printf("load from snapshot, key 0 %v\n", kv.data["0"])
}



//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	kv.kill <- true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.


	gob.Register(Op{})
	gob.Register(Reply{})

	kv := new(RaftKV)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if Debug > 0 {
		kv.logger.InitLogger(os.Stdout, os.Stdout, os.Stderr, os.Stderr)
	}else {
		kv.logger.InitLogger(ioutil.Discard, ioutil.Discard, os.Stderr, os.Stderr)
	}


	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kill = make(chan bool, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]*PendingOps)
	kv.stamps = make(map[int64]int64)

	kv.loadSnapShot(kv.persister.ReadSnapshot())

	go func() {
		for {
			select {
			case msg := <- kv.applyCh:
				kv.receiveApply(&msg)
			case <- kv.kill:
				return

			}

		}
	}()
	return kv
}
