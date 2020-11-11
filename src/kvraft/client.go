package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int   // leader的地址
	clientId int64 // 每个客户端都有一个唯一id
	opId     int64 // 每个操作都需要有一个唯一的id
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.opId = 1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs

	args.Key = key

	for {
		var reply GetReply

		if ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			} else if reply.Err == ErrTimeOut {
				DPrintf("Op:Get,Key:%v,Err:%v,retry...\n", key, reply.Err)
			} else {
				DPrintf("Op:Get,Key:%v,Err:%v\n", key, reply.Err)
				break
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}

	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op

	args.ClientId = ck.clientId
	args.OpId = atomic.AddInt64(&ck.opId, 1)

	for {
		var reply PutAppendReply

		if ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err == OK {
				if value == "15" {
					fmt.Printf("Op:%v,Key:%v,Value:%v,err:%v\n", args.Op, args.Key, args.Value, reply.Err)
				}

				break
			} else if reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			} else if reply.Err == ErrTimeOut {
				DPrintf("Op:%v,Key:%v,Value:%v,err:%v,retry...\n", args.Op, args.Key, args.Value, reply.Err)
			} else {
				DPrintf("Op:%v,Key:%v,Value:%v,err:%v\n", args.Op, args.Key, args.Value, reply.Err)
				break
			}
		} else { // 当前leader RPC不可用
			DPrintf("Op:%v,Key:%v,Value:%v,RPC error\n", args.Op, args.Key, args.Value)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
