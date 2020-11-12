package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

const TIMEOUT = time.Second * 3

const (
	OpGet    = 0
	OpAppend = 1
	OpPut    = 2
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	Key   string
	Value string

	// 实现命令执行幂等性
	ClientId int64
	OpId     int64
}

type OpWait struct {
	done chan bool
	op   Op
}

// 维护一个状态机
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	data      map[string]string

	opWaitQueue   map[int][]OpWait
	clientOpIndex map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op

	op.Type = OpGet
	op.Key = args.Key

	err := kv.exec(op)
	if err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			DPrintf("The key(%v) is not exist\n", args.Key)
			reply.Err = ErrNoKey
		}
	} else {
		DPrintf("Get key failure,err:%v\n", err)
		reply.Err = err
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op

	if args.Op == "Put" {
		op.Type = OpPut
	} else if args.Op == "Append" {
		op.Type = OpAppend
	} else {
		reply.Err = ErrWrongMethod
		return
	}

	op.Key = args.Key
	op.Value = args.Value

	op.ClientId = args.ClientId
	op.OpId = args.OpId

	reply.Err = kv.exec(op)

	// 检查是否为leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) exec(op Op) Err {
	opIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// DPrintf("server%d is not leader\n", kv.me)
		return ErrWrongLeader
	}

	// 使用raft写日志，等待提交成功
	var opWait OpWait
	opWait.done = make(chan bool)
	opWait.op = op

	kv.mu.Lock()
	kv.opWaitQueue[opIndex] = append(kv.opWaitQueue[opIndex], opWait)
	kv.mu.Unlock()

	ok := false
	timeout := false

	timer := time.NewTimer(TIMEOUT)
	select {
	case ok = <-opWait.done:
	case <-timer.C:
		DPrintf("wait operation apply to state machine exceeds timeout....\n")
		timeout = true
	}

	if op.Value == "15" {
		DPrintf("...key/value:%v/%v,opIndex:%v\n", op.Key, op.Value, opIndex)
	}

	kv.mu.Lock()
	delete(kv.opWaitQueue, opIndex)
	kv.mu.Unlock()

	if timeout {
		return ErrTimeOut
	}

	if !ok {
		return ErrDup
	}

	return OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 命令已在raft中提交
// 1.应用到状态机
// 2.通知
func (kv *KVServer) Apply(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var op Op
	op = msg.Command.(Op)

	dup := false

	// 处理重复命令
	// 如果命令已经被执行过，那么就不执行
	if op.Type != OpGet && kv.clientOpIndex[op.ClientId] >= op.OpId {
		DPrintf("Duplicate operation %d %d\n", kv.clientOpIndex[op.ClientId], op.OpId)
		dup = true
	} else {
		switch op.Type {
		case OpPut:
			DPrintf("Put Key/Value %v/%v\n", op.Key, op.Value)
			kv.data[op.Key] = op.Value
		case OpAppend:
			DPrintf("Append Key/Value %v/%v\n", op.Key, op.Value)
			kv.data[op.Key] += op.Value
		default:
		}
		kv.clientOpIndex[op.ClientId] = op.OpId
	}

	// 通知
	for _, opWait := range kv.opWaitQueue[msg.CommandIndex] {
		if dup {
			opWait.done <- false
		} else {
			opWait.done <- true
		}
	}

	// 检查日志是否过大，如果过大，那么就生成快照
	if kv.persister.RaftStateSize() >= kv.maxraftstate {

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.data = make(map[string]string)

	kv.opWaitQueue = make(map[int][]OpWait)
	kv.clientOpIndex = make(map[int64]int64)

	// 读取快照
	kv.rf.ReadSnapshot(&kv.data)

	// You may need initialization code here.
	go func() {
		for msg := range kv.applyCh {
			kv.Apply(msg)
		}
	}()

	return kv
}
