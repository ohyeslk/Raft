package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
//type PutAppendArgs struct {
//	// You'll have to add definitions here.
//	Key   string
//	Value string
//	Op    string // "Put" or "Append"
//	// You'll have to add definitions here.
//	// Field names must start with capital letters,
//	// otherwise RPC will break.
//}
//
//type PutAppendReply struct {
//	WrongLeader bool
//	Err         Err
//}
//
//type GetArgs struct {
//	Key string
//	// You'll have to add definitions here.
//}
//
//type GetReply struct {
//	WrongLeader bool
//	Err         Err
//	Value       string
//}

type OpType int
const (
	PUT	OpType = 1 + iota
	APPEND
	GET
)

type Op struct {
	Type	OpType
	Key 	string
	Value 	string
	ClientId 	int64
	RequestId	int64
}

type Reply struct {
	Success bool
	Value 	string
}
