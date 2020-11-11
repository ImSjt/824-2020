package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	STOPPED      = "stopped"
	INITIALIZED  = "initialized"
	FOLLOWER     = "follower"
	CANDIDATE    = "candidate"
	LEADER       = "leader"
	SNAPSHOTTING = "snapshotting"
)

const (
	HeartbeatCycle  = time.Millisecond * 50
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int        // 当前的任期号
	votedFor    int        // 为其投票的候选者ID
	logs        []LogEntry // 日志

	// Volatile state on all servers
	commitIndex int // 最大的已提交的日志标号
	lastApplied int // 最大应用到状态机的日志标号

	// Volatile state on leaders
	nextIndex  []int // 下一个要发送给跟随者的日志下标
	matchIndex []int // 已经复制到跟随者的日志下标

	state          string        // 状态：领导、候选者、跟随者
	applyCh        chan ApplyMsg // 监听提交信息
	timer          *time.Timer   // 定时器，用于leader选举
	voteGrantedNum int           // 获得选票数
}

// 日志
type LogEntry struct {
	Command interface{} // 命令
	Term    int         // 该命令leader的任期号
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	// 当前任期号
	term = rf.currentTerm
	// 是否为leader
	isleader = (rf.state == LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 每次修改某些状态的时候，都需要持久化
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 候选者的任期号
	CandidateId int // 候选者的ID
	// 用于对比日志新旧
	LastLogIndex  int // 候选者最后一条日志索引
	LastTermIndex int // 候选者最后一条日志的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm
	VoteGranted bool // true表示候选者接受到选票
}

//
// example RequestVote RPC handler.
// 候选人投票请求
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 日志是否一样新
	logUpToData := true

	// 候选人必须包含所有已提交的日志，候选人的日志必须比我新
	// 如果日志已提交，那么它就存在群集中大多数server中，如果候选者的日志和大多数跟随者一样新，那么它就一定包含所有已提交的日志

	// 如何比较日志新否？
	// 通过任期号和最后一条日志索引
	// 如果任期号不同，那么任期号大的比较新
	// 如果任期号相同，那么日志索引大的比较新

	// 判断日志是否一样新
	if len(rf.logs) > 0 {
		lastLogIndex := len(rf.logs) - 1
		lastLog := rf.logs[lastLogIndex]
		if lastLog.Term > args.LastTermIndex ||
			(lastLog.Term == args.LastTermIndex && lastLogIndex > args.LastLogIndex) {
			logUpToData = false
		}
	}

	if args.Term < rf.currentTerm { // 如果候选者任期号小于当前任期号，那么就不投票给它
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm { // 多个候选者同时申请投票，只能给一个候选者投票
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToData {
			rf.votedFor = args.CandidateId
		}

		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
	} else { // case1：只有一个候选者；case2：选票被瓜分，候选者超时将term+1，然后重新申请选票
		rf.state = FOLLOWER // 变成跟随者
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if logUpToData {
			rf.votedFor = args.CandidateId
		}

		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
	}

	if reply.VoteGranted {
		// 持久化状态
		rf.persist()

		rf.resetTimer()
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader的编号
	PrevLogIndex int        // 上一条日志的索引
	PrevLogTerm  int        // 上一条日志的任期号
	Entries      []LogEntry // 日志条目
	LeaderCommit int        // 已提交的日志索引
}

type AppendEntriesReply struct {
	MatchIndex int  // 复制日志最后的索引
	Term       int  // 跟随者当前的任期号
	Success    bool // true表示跟随者包含PrevLogTerm和PrevLogIndex指示的日志条目
}

// 跟随者接受日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("sesrver%d append entries, term:%d, leaderid:%d, leadercommit:%d, num: %d\n", rf.me, args.Term, args.LeaderId, args.LeaderCommit, len(args.Entries))

	// 1.如果leader的任期号小于当前任期号，那么就失败，对方并不是leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 候选者可能收到leader的心跳包，如果收到，那么就立即变为跟随者
	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1

	// 接受到心跳，更新跟随者定时器
	rf.resetTimer()

	// 2.如果leader的上一条日志与当前不匹配，那么就失败
	// fmt.Printf("prev log index:%d\n", args.PrevLogIndex)
	if args.PrevLogIndex >= 0 {
		if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
	} else if args.PrevLogIndex == -1 {
		// do nothing
	} else {
		// error
		fmt.Printf("prev log index error\n")
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 3.覆盖旧日志
	// 4.添加新日志
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)

	// 5.更新提交日志索引
	lastCommitIndex := rf.commitIndex
	if args.LeaderCommit < len(rf.logs) {
		rf.commitIndex = args.LeaderCommit
	} else {
		rf.commitIndex = len(rf.logs) - 1
	}

	// 如果有新的提交日志，那么就将日志应用到状态机
	if rf.commitIndex != lastCommitIndex {
		go rf.commitLog(rf.commitIndex)
	}

	reply.MatchIndex = len(rf.logs) - 1
	reply.Term = rf.currentTerm
	reply.Success = true

	// 持久化状态
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	if rf.state == LEADER {
		isLeader = true
		rf.logs = append(rf.logs, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.logs)

		// 持久化
		rf.persist()
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// 退出goroutine
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 领导者提交日志
func (rf *Raft) commitLog(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if commitIndex >= len(rf.logs) {
		commitIndex = len(rf.logs) - 1
	}

	rf.commitIndex = commitIndex // 在下一个心跳会将日志提交

	// commitIndex对应start时候返回的index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: i + 1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

// 向指定跟随者发送附加日志
func (rf *Raft) SendAppendEntriesToFollwer(server int) {
	// 向跟随者发送附加日志
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
	}

	if args.PrevLogIndex >= 0 {
		// fmt.Printf("prev log index:%d, log len:%d\n", args.PrevLogIndex, len(rf.logs))
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}

	if rf.nextIndex[server] < len(rf.logs) {
		args.Entries = rf.logs[rf.nextIndex[server]:]
	}

	go func(server int, args AppendEntriesArgs) {
		var reply AppendEntriesReply

		if ok := rf.sendAppendEntries(server, &args, &reply); ok {
			heartbeat := (len(args.Entries) == 0)
			rf.handleAppendEntriesReply(server, heartbeat, reply)
		}
	}(server, args)
}

// 向所有的跟随者发送附加日志
func (rf *Raft) SendAppendEntriesToAllFollwer() {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		rf.SendAppendEntriesToFollwer(server)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, heartbeat bool, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1

		rf.resetTimer()

		return
	}

	if reply.Success {
		// 如果是心跳包，那么就不判断是否是否需要提交
		if heartbeat {
			return
		}

		rf.nextIndex[server] = reply.MatchIndex + 1 // 下一个应该复制的日志索引
		rf.matchIndex[server] = reply.MatchIndex    // 已经复制的日志索引

		// 判断该日志是否已经复制到大多数server了，如果是，那么就可以提交
		commitIndex := rf.matchIndex[server]
		count := 1
		// 只有当前任期的日志才能被提交，根据日志匹配特征，之前的日志也会被提交
		if rf.logs[commitIndex].Term == rf.currentTerm {
			for i := 0; i < len(rf.nextIndex); i++ {
				if i == rf.me {
					continue
				}

				// 判断是否已经复制
				if rf.matchIndex[i] >= commitIndex {
					count++
				}

				// 判断是否已经复制到大多数server
				if count >= majority(len(rf.peers)) {
					go rf.commitLog(commitIndex)
					break
				}
			}
		}
	} else { // 如果发送失败，那么往后移动一个重新发送
		// fmt.Printf("next index:%d -> -1\n", rf.nextIndex[server])
		if rf.nextIndex[server] > 0 {
			rf.nextIndex[server] -= 1
		}

		rf.SendAppendEntriesToFollwer(server) // 重发
	}
}

func (rf *Raft) resetTimer() {
	// 启动一个定时器
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 1000)

		go func(rf *Raft) {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}(rf)
	}

	// 设置超时时间
	// 对于leader需要定期发送心跳
	// 对于跟随者，如果过长时间没有收到心跳，那么就变成候选者
	// 对于候选者，如果过长时间没有选举完成，那么重新开始一轮选举
	timeout := HeartbeatCycle
	if rf.state != LEADER {
		timeout = time.Millisecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}

	rf.timer.Reset(timeout)
}

func (rf *Raft) resetTimerQuick() {
	rf.timer.Reset(0)
}

func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		// 发送日志与心跳
		// fmt.Printf("server%d send heart beat\n", rf.me)
		rf.SendAppendEntriesToAllFollwer()
	} else {
		// 开始选举
		rf.state = CANDIDATE  // 成为候选者
		rf.currentTerm += 1   // 增加任期号
		rf.votedFor = rf.me   // 将票投给自己
		rf.voteGrantedNum = 1 // 获得选票个数

		// 持久化
		rf.persist()

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
		}

		if len(rf.logs) > 0 {
			args.LastTermIndex = rf.logs[args.LastLogIndex].Term
		}

		// fmt.Printf("server%d want to be leader, term:%d\n", rf.me, rf.currentTerm)
		// 并行向所有server申请投票
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}

			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply

				if ok := rf.sendRequestVote(server, &args, &reply); ok {
					rf.handleVoteResult(reply)
				}
			}(server, args)
		}
	}

	// 重置超时时间
	rf.resetTimer()
}

func majority(n int) int {
	return n/2 + 1
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 忽略旧的任期号
	if reply.Term < rf.currentTerm {
		return
	}

	// 如果存在任期号大于候选者的任期号，那么候选者就变成跟随者
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()

		return
	}

	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.voteGrantedNum += 1

		// 如果获得大多数选票，那么就成为leader
		if rf.voteGrantedNum >= majority(len(rf.peers)) {
			rf.state = LEADER

			// fmt.Printf("server%d to be leader, term:%d\n", rf.me, rf.currentTerm)

			// 初始化所有跟随者的日志记录
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				rf.nextIndex[i] = len(rf.logs)         // 写一个要发送的日志索引
				rf.matchIndex[i] = rf.nextIndex[i] - 1 // 已复制的日志索引
			}

			// 重置定时器，开始发送心跳包，立即发送心跳
			// rf.resetTimer()
			rf.resetTimerQuick()
		}

		return
	}
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 创建一个goroutine，该后台goroutine将在有一段时间没有收到其他对等方的请求时发送RequestVote RPC来定期启动领导者选举。
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = FOLLOWER
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	// 恢复状态
	rf.readPersist(persister.ReadRaftState())

	// 持久化状态
	rf.persist()

	// 启动一个goroutine，处理leader选举与日志处理
	rf.resetTimer()

	return rf
}
