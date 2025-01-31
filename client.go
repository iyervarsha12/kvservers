package kvpaxos

import (
	"crypto/rand"
	"math/big"
	//"time"
	"cs651/labrpc"
	//"fmt"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkid int 
	requestno int 
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
	ck.clerkid = int(nrand())
	ck.requestno = 1
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	//fmt.Println("-----------GET STARTER request for key",key)
	// You will have to modify this function.
	args := GetArgs{}
	args.ClientID =  ck.clerkid
	args.RequestID = ck.requestno
	args.Key = key 
	ck.requestno++

	// TODO: we should be calling the leader server first, and then the rest in a round robin fashion.
	// TODO: Try each server ck.servers[i]. Trial until success. 
	for {
		//fmt.Println("GET Round robining again through ", len(ck.servers), "servers")
		for i:=0; i<len(ck.servers); i++ {
			var reply GetReply
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			//fmt.Println("-----------GET request to server",i, "for key",key)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				//fmt.Println("-----------GET request to server",i, "for key",key, "HANDLED")
				return reply.Value
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
	
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//fmt.Println("-----------PUTAPPEND STARTER request for key value",key, value)
	args := PutAppendArgs{}
	args.RequestID = ck.requestno
	args.ClientID = ck.clerkid
	args.Key = key
	args.Value = value
	args.Op = op
	ck.requestno++
	for {
		// Keep round robin-ing through servers
		//fmt.Println("PUTAPPEND Round robining again through ", len(ck.servers), "servers")
		for i:=0; i<len(ck.servers); i++ {
			var reply PutAppendReply
			//failed request? not the leader

			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			//fmt.Println("-----------PUTAPPEND",op, "request to server",i, "for key value",key, value)
			if ok && (reply.Err == OK) {
				//fmt.Println("-----------PUTAPPEND",op, "request to server",i, "for key value",key, value, "HANDLED")
				return
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
