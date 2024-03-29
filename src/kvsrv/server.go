package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Client send request to server by seq, the Client have already received reply before this seq
// thus, Server can only store the latest seq for and related key - value for each client
type dupTable struct {
	seq   int
	value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data        map[string]string
	clientTable map[int64]*dupTable
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.clientTable[args.ClientId]; !ok {
		// When this client has no write operations, the data it reads is always the current server Value
		reply.Value = kv.data[args.Key]
		return
	}
	// if client is new, create the map of ClientId -> duplicateTable
	//if _, ok := kv.clientTable[args.ClientId]; !ok {
	//	kv.clientTable[args.ClientId] = &dupTable{-1, ""}
	//}
	dt := kv.clientTable[args.ClientId]

	// duplicate request
	if dt.seq == args.Seq {
		reply.Value = dt.value
		return
	}
	dt.seq = args.Seq
	dt.value = kv.data[args.Key]
	reply.Value = dt.value

	// DPrintf("Server: ClientId %v Get reply %v and dt.seq is %v, value is %v\n", args.ClientId, args.Seq, dt.seq, dt.value)

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if client is new, create the map of ClientId -> duplicateTable
	if _, ok := kv.clientTable[args.ClientId]; !ok {
		kv.clientTable[args.ClientId] = &dupTable{-1, ""}
	}
	dt := kv.clientTable[args.ClientId]

	// duplicate request
	if dt.seq == args.Seq {
		return
	}
	dt.seq = args.Seq
	// remember put operation does not return value, so the dt.value is not important
	dt.value = ""
	kv.data[args.Key] = args.Value

	// DPrintf("Server: ClientId %v Put reply %v and dt.seq is %v, value is %v\n", args.ClientId, args.Seq, dt.seq, dt.value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Ensure that resending does not cause the server to deal with the request twice
	// if client is new, create the map of ClientId -> duplicateTable
	if _, ok := kv.clientTable[args.ClientId]; !ok {
		kv.clientTable[args.ClientId] = &dupTable{-1, ""}
	}
	dt := kv.clientTable[args.ClientId]

	// duplicate request
	if dt.seq == args.Seq {
		reply.Value = dt.value
		return
	}
	dt.seq = args.Seq
	dt.value = kv.data[args.Key]
	// return old value
	reply.Value = dt.value

	// notice: append operation is Add operation
	kv.data[args.Key] = kv.data[args.Key] + args.Value

	// DPrintf("Server: ClientId %v Append reply %v and dt.seq is %v, value is %v\n", args.ClientId, args.Seq, dt.seq, dt.value)

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.clientTable = make(map[int64]*dupTable)

	return kv
}
