package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Args struct {
	ClientId int32
	RequestId int64
}

type PutOrAppend int

const (
	PutOp PutOrAppend = iota
	AppendOp
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Type  PutOrAppend
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Args
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
