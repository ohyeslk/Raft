package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	leader 	int	//raft leader index
	me 	int64	// client id
	reqId 	int64	// increasing monotonically
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.leader = 0
	ck.me = nrand()
	ck.reqId = 0
	return ck
}

// keep sending the request to server, until get reply successfully
func (ck *Clerk) execute(req Op) string{
	req.ClientId = ck.me
	req.RequestId = atomic.AddInt64(&ck.reqId, 1)

	i := ck.leader	// start from last saved leader id
	n := len(ck.servers)	// n stands for server num
	for {
		var reply Reply
		ok := ck.servers[i].Call("RaftKV.ExecuteRequest", req, &reply)

		if ok && reply.Success {
			// get reply successfully
			// update ck.leader and return value
			ck.leader = i
			return reply.Value
		}else {
			// fail, try next server
			i = (i + 1) % n
		}
	}
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.execute(Op{Type: GET, Key: key})
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Put(key string, value string) {
	ck.execute(Op{Type: PUT, Key: key, Value: value})
}
func (ck *Clerk) Append(key string, value string) {
	ck.execute(Op{Type: APPEND, Key: key, Value: value})
}
