package shardmaster

import (
	"fmt"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	JOIN  = 1
	LEAVE = 2
	MOVE  = 3
	QUERY = 4
)

const TIMEOUT = time.Second * 3

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opWaitQueue   map[int][]OpWait
	clientOpIndex map[int64]int64

	configs []Config // indexed by config num
}

// 使用raft保持多个master一致
type Op struct {
	// Your data here.
	Type int
	Args interface{}

	ClientId int64
	OpId     int64
}

type OpWait struct {
	done chan bool
	op   Op
}

func (sm *ShardMaster) makeNewConfig() {
	size := len(sm.configs)
	config := Config{}
	config.Num = size
	config.Shards = sm.configs[size-1].Shards
	config.Groups = make(map[int][]string)

	for key, value := range sm.configs[size-1].Groups {
		servers := make([]string, len(value))
		copy(servers, value)
		config.Groups[key] = servers
	}

	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) rebalance() {
	last := len(sm.configs) - 1

	groupNum := len(sm.configs[last].Groups)
	shardNum := len(sm.configs[last].Shards)

	if groupNum == 0 {
		for i := range sm.configs[last].Shards {
			sm.configs[last].Shards[i] = 0
		}
		return
	}

	num := shardNum / groupNum
	if shardNum%groupNum != 0 {
		num++
	}

	groupIds := []int{}
	for id, _ := range sm.configs[last].Groups {
		groupIds = append(groupIds, id)
	}

	index := 0
	j := 0
	for i := range sm.configs[last].Shards {
		sm.configs[last].Shards[i] = groupIds[index]

		j++
		if j >= num {
			j = 0
			index++
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	var op Op

	op.Type = JOIN
	op.Args = args
	op.ClientId = args.ClientId
	op.OpId = args.OpId

	if ok := sm.exec(op); ok {
		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	var op Op

	op.Type = LEAVE
	op.Args = args
	op.ClientId = args.ClientId
	op.OpId = args.OpId

	if ok := sm.exec(op); ok {
		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	var op Op

	op.Type = MOVE
	op.Args = args
	op.ClientId = args.ClientId
	op.OpId = args.OpId

	if ok := sm.exec(op); ok {
		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	var op Op

	op.Type = QUERY
	op.Args = args

	if ok := sm.exec(op); ok {
		sm.mu.Lock()
		defer sm.mu.Unlock()

		if args.Num <= -1 || args.Num >= len(sm.configs) {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}

		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) exec(op Op) bool {
	opIndex, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}

	// 使用raft写日志，等待提交成功
	var opWait OpWait
	opWait.done = make(chan bool)
	opWait.op = op

	sm.mu.Lock()
	sm.opWaitQueue[opIndex] = append(sm.opWaitQueue[opIndex], opWait)
	sm.mu.Unlock()

	ok := false
	timer := time.NewTimer(TIMEOUT)
	select {
	case ok = <-opWait.done:
	case <-timer.C:
		fmt.Printf("wait operation apply to state machine exceeds timeout....\n")
		return false
	}

	sm.mu.Lock()
	delete(sm.opWaitQueue, opIndex)
	sm.mu.Unlock()

	return ok
}

func (sm *ShardMaster) handleJoinOp(args *JoinArgs) {
	sm.makeNewConfig()
	last := len(sm.configs) - 1

	for key, value := range args.Servers {
		sm.configs[last].Groups[key] = value
	}

	// 重新平衡
	sm.rebalance()
}

func (sm *ShardMaster) handleLeaveOp(args *LeaveArgs) {
	sm.makeNewConfig()
	last := len(sm.configs) - 1

	for _, id := range args.GIDs {
		delete(sm.configs[last].Groups, id)
	}

	// 重新平衡
	sm.rebalance()
}

func (sm *ShardMaster) handleMoveOp(args *MoveArgs) {
	sm.makeNewConfig()
	last := len(sm.configs) - 1

	sm.configs[last].Shards[args.Shard] = args.GID
}

func (sm *ShardMaster) Apply(msg raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var op Op
	op = msg.Command.(Op)

	if op.Type != QUERY && sm.clientOpIndex[op.ClientId] >= op.OpId {
		fmt.Printf("Duplicate operation %d %d\n", sm.clientOpIndex[op.ClientId], op.OpId)
	} else {
		switch op.Type {
		case JOIN:
			args := op.Args.(*JoinArgs)
			sm.handleJoinOp(args)

		case LEAVE:
			args := op.Args.(*LeaveArgs)
			sm.handleLeaveOp(args)

		case MOVE:
			args := op.Args.(*MoveArgs)
			sm.handleMoveOp(args)

		default:
		}
	}

	for _, opWait := range sm.opWaitQueue[msg.CommandIndex] {
		opWait.done <- true
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.opWaitQueue = make(map[int][]OpWait)
	sm.clientOpIndex = make(map[int64]int64)

	// Your code here.
	go func() {
		for msg := range sm.applyCh {
			sm.Apply(msg)
		}
	}()

	return sm
}
