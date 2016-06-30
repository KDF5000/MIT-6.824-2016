package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
//	"fmt"
	"time"
	"container/list"
)

const Debug = 1

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
	Client    int64
	Sequence  int
	Key       string
	Value     string
	Type      string
	Err       Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data           map[string]string
	executedID     map[int64]int
	notify         map[int][]chan Op
	appliedEntry   int
	
	Recv           chan raft.ApplyMsg
	Msgs           *list.List
	QuitCH         chan bool

}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{args.Client, args.Sequence, args.Key, "", "Get", ""}
	kv.mu.Lock()
	if _, ok := kv.executedID[args.Client]; !ok {
	    kv.executedID[args.Client] = -1
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
	    kv.mu.Unlock()
	    reply.WrongLeader = true
	    ////fmt.Printf("%d is not leader.\n", kv.me)
	    return
	}
	////fmt.Printf("Client %d Call server %d Get Key Seq: %d, log index: %d\n", args.Client, kv.me,  args.Sequence, index)
	////fmt.Printf("%d is leader\n", kv.me)
	if _, ok := kv.notify[index]; !ok {
	    kv.notify[index] = make([]chan Op, 0)
	    ////fmt.Printf("Client %d make notification channel on server %d in log index %d\n", args.Client, kv.me, index)
	}
	notifyMe := make(chan Op)
	kv.notify[index] = append(kv.notify[index], notifyMe)
	kv.mu.Unlock()
	////fmt.Printf("%d is waiting for commit command Client:%d, Seq:%d \n", kv.me, args.Client, args.Sequence)
	
	var executedOp Op
	var notified = false
	for {
	    select {
	    case executedOp = <- notifyMe:
	        notified = true
	        break
	    case <- time.After(10*time.Millisecond):
	        kv.mu.Lock()
	        if currentTerm, _ := kv.rf.GetState(); term != currentTerm {
		    if kv.appliedEntry < index {
		        reply.WrongLeader = true
			delete(kv.notify, index)
			kv.mu.Unlock()
			return
		    }
		}
		kv.mu.Unlock()
	    }
	    if notified {
	        break
	    }
	}
	
//	executedOp := <- notifyMe
	////fmt.Printf("Client %d Seq %d Commited on server %d\n", args.Client, args.Sequence, kv.me)
	reply.WrongLeader = false
	if executedOp.Client != op.Client || executedOp.Sequence != op.Sequence {
	    reply.Err = "FailCommit"
	} else {
	    if executedOp.Err == ErrNoKey {
	        reply.Err = ErrNoKey
	    } else {
	        reply.Value = executedOp.Value
		reply.Err = OK
	    }
	}
	////fmt.Printf("reply.WrongLeader: %t, reply.Err: %s, server: %d\n", reply.WrongLeader, reply.Err, kv.me)
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Client, args.Sequence, args.Key, args.Value, args.Op, ""}
	kv.mu.Lock()
	if _, ok := kv.executedID[args.Client]; !ok {
	    ////fmt.Printf("Client %d set seq as -1 on server %d\n", args.Client, kv.me)
	    kv.executedID[args.Client] = -1
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
	    kv.mu.Unlock()
	    reply.WrongLeader = true
	    ////fmt.Printf("%d is not leader.\n", kv.me)
	    return
	}
	////fmt.Printf("Client %d Call server PutAppend: %s, Seq: %d, log index: %d\n", args.Client, args.Op, args.Sequence, index)
	////fmt.Printf("%d is leader\n", kv.me)
	if _, ok := kv.notify[index]; !ok {
	    kv.notify[index] = make([]chan Op, 0)
	    ////fmt.Printf("Client %d make notification channel on server %d in log index %d\n", args.Client, kv.me, index)
	}
	notifyMe := make(chan Op)
	kv.notify[index] = append(kv.notify[index], notifyMe)
	kv.mu.Unlock()
	////fmt.Printf("%d is waiting for commit command Client:%d, Seq:%d \n", kv.me, args.Client, args.Sequence)
	
	var executedOp Op
	notified := false
	for {
	    select {
	    case executedOp = <- notifyMe:
	        notified = true
	        break
	    case <- time.After(10*time.Millisecond):
	        kv.mu.Lock()
	        if currentTerm, _ := kv.rf.GetState(); term != currentTerm {
		    if kv.appliedEntry < index {
		        reply.WrongLeader = true
			delete(kv.notify, index)
		        kv.mu.Unlock()
		        return
		    }
		}
		kv.mu.Unlock()
	    }
	    if notified {
	        break
	    }
	}

//	executedOp := <- notifyMe

	////fmt.Printf("Client %d Seq %d Commited on server %d\n", args.Client, args.Sequence, kv.me)
	reply.WrongLeader = false
	////fmt.Printf("-------------------------------%t\n", reply.WrongLeader)
	if executedOp.Client != op.Client || executedOp.Sequence != op.Sequence {
	    reply.Err = "FailCommit"
	} else {
	    reply.Err = OK
	}
	////fmt.Printf("reply.WrongLeader: %t, reply.Err: %s, server: %d\n", reply.WrongLeader, reply.Err, kv.me)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.QuitCH)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.executedID = make(map[int64]int)
	kv.notify = make(map[int][]chan Op)
	kv.appliedEntry = 0
	
	kv.QuitCH = make(chan bool)
	kv.Msgs = list.New()
	kv.Recv = make(chan raft.ApplyMsg)
	
	go func() {
	//    for range kv.applyCh {
	        ////fmt.Printf("Receive apply msg, server:%d, client:%d, seq:%d, index:%d\n", kv.me, msg.Command.(Op).Client, msg.Command.(Op).Sequence, msg.Index)
	  //      go kv.apply()
	       
	   // }
	   for {
	       var (
	           recvChan chan raft.ApplyMsg
	   	      recvVal  raft.ApplyMsg
	       )
	       if kv.Msgs.Len() > 0 {
	           recvChan = kv.Recv
	   	      recvVal = kv.Msgs.Front().Value.(raft.ApplyMsg)
	       }
	       select {
	       case msg := <- kv.applyCh:
	   	      kv.Msgs.PushBack(msg)
	       case recvChan <- recvVal:
	   	      kv.Msgs.Remove(kv.Msgs.Front())
	       case <- kv.QuitCH:
	           return
	       }
	   }
	} ()
	
	go func() {
	  for {
	     select {
		case msg := <- kv.Recv:
		    kv.mu.Lock()
		    kv.applyCommand(msg)
		    kv.mu.Unlock()
		case <- kv.QuitCH:
		    return
		}
	    }
	} ()
	
	return kv
}

func (kv *RaftKV) apply() {
    kv.mu.Lock()
    kv.appliedEntry++
    msg := raft.ApplyMsg{Index: kv.appliedEntry, Command: kv.rf.Log[kv.appliedEntry].Command}
    kv.applyCommand(msg)
    kv.mu.Unlock()
    
}

func (kv *RaftKV) applyCommand(msg raft.ApplyMsg) {
    op := msg.Command.(Op)
    logIndex := msg.Index
    clientID := op.Client
    opSequence := op.Sequence
    res := Op{clientID, opSequence, op.Key, op.Value, op.Type, op.Err}
    
//    kv.mu.Lock()

    ////fmt.Printf("Server:%d, Command is %s, opSequence: %d, kv.executedID[client]: %d\n", kv.me, op.Type, opSequence, kv.executedID[clientID])
    if _, ok := kv.executedID[clientID]; !ok {
        kv.executedID[clientID] = -1
    }
    
    if opSequence > kv.executedID[clientID] {
        ////fmt.Printf("Log Index:%d\n", logIndex)
        switch op.Type {
	case "Get":
	    v, ok := kv.data[op.Key]
	    if !ok {
	        res.Err = ErrNoKey
	    } else {
	        res.Value = v
	    }
	    ////fmt.Printf("Server:%d Get Value applied Client:%d, Seq: %d. Key:%s, Value:%s\n", kv.me, clientID, opSequence, op.Key, op.Value)
//	    ////fmt.Printf("Server:%d Get Value applied Client:%d, Seq: %d. Key:%s\n", kv.me, clientID, opSequence, op.Key)
	case "Put":
	    kv.data[op.Key] = op.Value
	    ////fmt.Printf("Server:%d Put Value applied Client:%d, Seq: %d. Key:%s, Value:%s\n", kv.me, clientID, opSequence, op.Key, op.Value)
	case "Append":
	    _, ok := kv.data[op.Key]
	    if !ok {
	        kv.data[op.Key] = op.Value
	    } else {
	        kv.data[op.Key] += op.Value
	    }
//	    ////fmt.Printf("Server %d Append Value applied Client:%d, Seq: %d. Key:%s, append:%s\n", kv.me, clientID, opSequence, op.Key, op.Value)
	    ////fmt.Printf("Server %d Append Value applied Client:%d, Seq: %d.Key:%s, ValueAfterAppend:%s, append:%s\n", kv.me, clientID, opSequence, op.Key, kv.data[op.Key], op.Value)
	}
	kv.executedID[clientID] = opSequence
    } else {
        ////fmt.Printf("Server %d Already seen Client : %d, Seq: %d.\n",kv.me,  clientID, opSequence)
        if op.Type == "Get" {
	    v, ok := kv.data[op.Key]
	    if !ok {
	        res.Err = ErrNoKey
	    } else {
	        res.Value = v
	    }
	}
    }
    
//    kv.mu.Unlock()
    if _, ok := kv.notify[logIndex]; !ok {
        ////fmt.Printf("server %d, client:%d, seq:%d, index:%d No one notify and return\n", kv.me, clientID, opSequence, logIndex)
//	kv.mu.Lock()
        return
    }
    
//    kv.mu.Unlock()
    for _,  c := range kv.notify[logIndex] {
       // tmp := Op{res.Client, res.Sequence, res.Key, res.Value, res.Type, res.Err}
	////fmt.Printf("-----------------------tmp made and put on channel in  server %d, client:%d, seq:%d\n", kv.me, clientID, opSequence)
	kv.mu.Unlock()
        c <- res
	kv.mu.Lock()
	////fmt.Printf("Channel taken on server %d, client:%d, seq:%d\n", kv.me, clientID, opSequence)
    }
//    kv.mu.Lock()

    delete(kv.notify, logIndex)
    
//    kv.mu.Unlock()

}
