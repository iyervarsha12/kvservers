package kvpaxos

import (
	"fmt"
	"log"
	"plugin"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"cs651/labgob"
	"cs651/labrpc"

	omnipaxoslib "cs651-gitlab.bu.edu/cs651-fall24/omnipaxos-lib/omnipaxos-lib"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string // Type of operation
  	Key string 
  	Value string
  	ClientID int
  	RequestID int
}

// Make an Op for get(). RequestID exists to check for dups.
func GetOpMake(key string, fromclient int, requestID int) Op {
	op := Op{}
	op.Type = "GET"
	op.Key = key
	op.ClientID = fromclient
	op.RequestID = requestID
	op.Value = ""
	return op
}
  
// Make an Op for put(). RequestID exists to check for dups.
func PutAppendOpMake(key string, value string, fromclient int, requestID int, optype string) Op {
	op := Op{}
	op.Type = optype // either Put or Append
	op.Key = key
	op.Value = value
	op.ClientID = fromclient
	op.RequestID = requestID
	return op
}

type SeenRequestStructKey struct {
	clientID int
	requestID int 
}

type SeenRequestsStructValue struct {
	optype string // of last seen request 
	putappendreply PutAppendReply
	getreply GetReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      omnipaxoslib.IOmnipaxos
	applyCh chan omnipaxoslib.ApplyMsg
	dead    int32 // set by Kill()

	enableLogging int32

	// Your definitions here.
	seenrequests map[SeenRequestStructKey]SeenRequestsStructValue //key clientID int, struct with response for a certain requestID
	// TODO DELETE the request from the map once you're done providing a response 
	kvstore map[string]string
	newestrequest map[int]int //client key, newest request served value 
	receivedCh map[SeenRequestStructKey]chan Op //For each client-request key, have channel value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Println("						Server ", kv.me, "got Get request with key, requestID, clientID as ", args.Key, args.RequestID, "-",args.ClientID)
	kv.mu.Lock()
	// GetOpMake(key string, fromclient int, requestID int)
	op := GetOpMake(args.Key, args.ClientID, args.RequestID)

	// Have we seen this request before? can move this to applych detect
	maptemp, ok := kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] 
	if (ok && maptemp.optype == "Get") {
		reply.Value = maptemp.getreply.Value
		reply.Err = maptemp.getreply.Err 
		kv.mu.Unlock()
		return
	}

	// Only serve new requests! 
	latestrequest, ok := kv.newestrequest[args.ClientID]
	if (ok && args.RequestID < latestrequest) {
		kv.mu.Unlock()
		return //stale request
	}

	_, isleader2 := kv.rf.GetState()
	if(!isleader2) {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return  
	}

	// proposal answer is 
	// Have not seen this request before. Call op server and get the value 
	// Make sure to fill in the reply from the value you got 
	_, _, ok2 := kv.rf.Proposal(op) 
	if (ok2!=true) {
		//fmt.Println("Server ", kv.me, "Get rejected Proposal() as it's not the leader")
		reply.Value = ""
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.receivedCh[SeenRequestStructKey{args.ClientID, args.RequestID}] = make(chan Op, 5)
	kv.mu.Unlock()
	//fmt.Println("Server ", kv.me, "Get accepted Proposal() as it's the leader, waiting for response from ReceivedCh[",args.ClientID,"]")
	// Free for other clients to call GET() now.
	// needs to constantly poll apply
	// If proposal has been accepted, commands that have been agreed upon arrive on applyCh

	select {
	case temp := <- kv.receivedCh[SeenRequestStructKey{args.ClientID, args.RequestID}] :
		//fmt.Println("Server ", kv.me, "Get Got a response from Receivedchannel (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value,",",  temp.RequestID, "-",temp.ClientID)
		// Only serve new requests! 
		latestrequest, ok := kv.newestrequest[args.ClientID]
		if (ok && args.RequestID < latestrequest) {
			//kv.mu.Unlock()
			return //stale request
		}
		if temp.ClientID == args.ClientID && temp.RequestID == args.RequestID { //sanity check
			_, isleader := kv.rf.GetState()
			if(!isleader) {
				reply.Value = ""
				reply.Err = ErrWrongLeader
				return  
			}
			
			// Do the actual database store.
			kv.mu.Lock()
			defer kv.mu.Unlock()
			ans, ok := kv.kvstore[args.Key]
			if (ok) {
				reply.Value = ans 
				reply.Err = OK 
				tempreply := SeenRequestsStructValue{}
				tempreply.optype = "Get"
				tempreply.getreply = *reply
				kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] = tempreply
				kv.newestrequest[args.ClientID] = args.RequestID
				//kv.mu.Unlock()
				fmt.Println("Server ", kv.me, "Get APPLIED (key, value, requestID, clientID) ", args.Key, ",", reply.Value,",",  temp.RequestID, "-",temp.ClientID)
		
				return
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
				tempreply := SeenRequestsStructValue{}
				tempreply.optype = "Get"
				tempreply.getreply = *reply
				kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] = tempreply
				kv.newestrequest[args.ClientID] = args.RequestID
				//kv.mu.Unlock()
				return
			}
		}
	
	case <-time.After(1 * time.Second):
		fmt.Println("Server ", kv.me, "Get receivedchannel timed out")
		reply.Value = ""
		reply.Err = ErrTimeout
		return
	}
}

// Just listen to replies forever and forward them to 'receivedCh'
func (kv *KVServer) ListenApplies() {
	for {
		x := <- kv.applyCh
		temp, _ := x.Command.(Op)
		//fmt.Println("Server ", kv.me, "listenapplies() Got response from Applychannel, (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value, ",", temp.RequestID, "-", temp.ClientID)
		// try out a receivedCh clientid-requestid based that gets dynamically allocated.
		// Indicate to Get() and PutAppend() that you've gotten a response 'temp' of type 'Op'
		//kv.mu.Lock()
		_, keyexists := kv.receivedCh[SeenRequestStructKey{temp.ClientID, temp.RequestID}]
		//kv.mu.Unlock()
		if (keyexists) {
			kv.receivedCh[SeenRequestStructKey{temp.ClientID, temp.RequestID}] <- temp 
			//fmt.Println("Server ", kv.me, "listenapplies() sent to receivedCh",temp.ClientID, "(type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key,",",  temp.Value, ",", temp.RequestID,"-", temp.ClientID)
		}
		/*
		map of channels, key is client-requestid, if key exists on map send on applych, else don't send. 
		type Op struct {
		Type string // Type of operation
		Key string 
		Value string
		ClientID int
		RequestID int
		}
		*/
		
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("							Server ", kv.me, "got", args.Op,  "PutAppend request with key, value, requestID, clientID as ", args.Key, ",", args.Value, ",", args.RequestID,"-", args.ClientID)
	
	kv.mu.Lock()
	op := PutAppendOpMake(args.Key, args.Value, args.ClientID, args.RequestID, args.Op)

	// Have we seen this request before? 
	maptemp, ok := kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] 
	if (ok && (maptemp.optype == "Put" || maptemp.optype == "Append")) {
		reply.Err = maptemp.putappendreply.Err 
		kv.mu.Unlock()
		return
	}

	// Only serve new requests! 
	latestrequest, ok := kv.newestrequest[args.ClientID]
	if (ok && args.RequestID < latestrequest) {
		kv.mu.Unlock()
		return //stale request
	}

	_, isleader2 := kv.rf.GetState()
	if(!isleader2) {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return  
	}

	_, _, ok2 := kv.rf.Proposal(op) 
	if (ok2!=true) {
		//fmt.Println("Server ", kv.me, "rejected PutAppend Proposal() as it's not the leader")
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.receivedCh[SeenRequestStructKey{args.ClientID, args.RequestID}] = make(chan Op, 5)
	kv.mu.Unlock()
	//fmt.Println("Server ", kv.me, "accepted PutAppend Proposal() as it's the leader, waiting for response from ReceivedCh[",args.ClientID,"]")

	select {
		case temp := <- kv.receivedCh[SeenRequestStructKey{args.ClientID, args.RequestID}] :
			//fmt.Println("Server ", kv.me, "PutAppend Got a response from Receivedchannel (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value, ",", temp.RequestID, "-", temp.ClientID)
			if temp.ClientID == args.ClientID && temp.RequestID == args.RequestID { //sanity check
				_, isleader := kv.rf.GetState()
				if(!isleader) {
					reply.Err = ErrWrongLeader
					return  
				}
				kv.mu.Lock() 	
				defer kv.mu.Unlock()
				if(args.Op == "Put") {
					kv.kvstore[args.Key] = args.Value
					reply.Err = OK 
					tempreply := SeenRequestsStructValue{}
					tempreply.optype = "Put"
					tempreply.putappendreply = *reply
					kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] = tempreply
					kv.newestrequest[args.ClientID] = args.RequestID
					//kv.mu.Unlock() 
					fmt.Println("Server ", kv.me, "Put APPLIED (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value, ",", temp.RequestID, "-", temp.ClientID)
					return
				} else if (args.Op == "Append") {
					ans, ok := kv.kvstore[args.Key]
					if (ok) { //key exists, need to append
						kv.kvstore[args.Key] = ans + args.Value
						reply.Err = OK 
						tempreply := SeenRequestsStructValue{}
						tempreply.optype = "Append"
						tempreply.putappendreply = *reply
						kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] = tempreply
						kv.newestrequest[args.ClientID] = args.RequestID
						//kv.mu.Unlock() 
						fmt.Println("Server ", kv.me, "Append APPLIED (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value, ",", temp.RequestID, "-", temp.ClientID)
					
						return
					} else { //empty key, is a put operation
						kv.kvstore[args.Key] = args.Value
						reply.Err = OK 
						tempreply := SeenRequestsStructValue{}
						tempreply.optype = "Append"
						tempreply.putappendreply = *reply
						kv.seenrequests[SeenRequestStructKey{args.ClientID, args.RequestID}] = tempreply
						kv.newestrequest[args.ClientID] = args.RequestID
						//kv.mu.Unlock() 
						fmt.Println("Server ", kv.me, "(previously empty) Append APPLIED (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value, ",", temp.RequestID, "-", temp.ClientID)
					
						return 
					}

				} else {
					fmt.Println("Server ", kv.me, "WRONG OPCODE (type: key, value, requestID, clientID) ", temp.Type, ":", temp.Key, ",", temp.Value, ",", temp.RequestID, "-", temp.ClientID)
					reply.Err = ErrWrongOpcode
					//kv.mu.Unlock()
					return
				}
			}
		
		case <-time.After(1 * time.Second):
			fmt.Println("Server ", kv.me, "PutAppend receivedchannel timed out")
			reply.Err = ErrTimeout
			return
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via OmniPaxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying OmniPaxos
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the OmniPaxos state along with the snapshot.
// the k/v server should snapshot when OmniPaxos's saved state exceeds maxomnipaxosstate bytes,
// in order to allow OmniPaxos to garbage-collect its log. if maxomnipaxosstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister omnipaxoslib.Persistable, maxomnipaxosstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	// to enable logging
	// set to 0 to disable
	kv.enableLogging = 1

	// You may need initialization code here.

	kv.mu.Lock()
	kv.seenrequests = make(map[SeenRequestStructKey]SeenRequestsStructValue)
	kv.kvstore = make(map[string]string)
	kv.newestrequest = make(map[int]int)
	kv.newestrequest = make(map[int]int)
	kv.receivedCh = make(map[SeenRequestStructKey]chan Op)
	kv.mu.Unlock()

	/*
	seenrequests map[SeenRequestStructKey]SeenRequestsStructValue //key clientID int, struct with response for a certain requestID
	// TODO DELETE the request from the map once you're done providing a response 
	kvstore map[string]string
	newestrequest map[int]int //client key, newest request served value 
	receivedCh map[int]chan Op //For each client key, have channel value
	*/

	// kv.applyCh = make(chan omnipaxos.ApplyMsg)
	kv.applyCh = make(chan omnipaxoslib.ApplyMsg)

	p, err := plugin.Open(fmt.Sprintf("./omnipaxosmain-%s-%s.so", runtime.GOOS, runtime.GOARCH))
	if err != nil {
		panic(err)
	}
	xrf, err := p.Lookup("MakeOmnipaxos")
	if err != nil {
		panic(err)
	}

	mkrf := xrf.(func([]omnipaxoslib.Callable, int, omnipaxoslib.Persistable, chan omnipaxoslib.ApplyMsg) omnipaxoslib.IOmnipaxos)

	callables := make([]omnipaxoslib.Callable, len(servers))
	for i, s := range servers {
		callables[i] = s
	}

	// kv.rf = omnipaxos.Make(servers, me, persister, kv.applyCh)
	kv.rf = mkrf(callables, me, persister, kv.applyCh)

	// You may need initialization code here.

	go func() {
		for !kv.killed() {
			kv.ListenApplies()
		}
	}()

	return kv
}
