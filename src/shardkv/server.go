package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "container/list"
import "bytes"
import "time"
// import "fmt"


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client   int64
	Sequence int64
	
	Key      string
	Value    string
	
	Config   shardmaster.Config

	Data     map[string]string
	Shard    int
	ExecutedID map[int64]int64
	Type     string
	Err      Err
}

type OpReply struct {
	Client int64
	Sequence int64
	
	Value string

	WrongLeader bool
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	servers           []*labrpc.ClientEnd
	persister         *raft.Persister
	data              map[string]string
	executedID        map[int64]int64
	notify            map[int][]chan OpReply
	
	Recv              chan raft.ApplyMsg
	Msgs              *list.List
	QuitCH            chan bool
	
	lastIncludedIndex int
	lastIncludedTerm  int

	Configs           []shardmaster.Config
	Shards            [shardmaster.NShards]int
	mck               *shardmaster.Clerk
	ConfigNum         int
}

type SendShardArgs struct {
	Data       map[string]string
	Shard      int
	ExecutedID map[int64]int64
	Config     shardmaster.Config
	NewConfig  shardmaster.Config
}

type SendShardReply struct {
	WrongLeader bool
	Err        Err
}

func (kv *ShardKV) persist() (int, int, bool) {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	lastIncludedIndex := 0
	lastIncludedTerm := 0
	r := bytes.NewBuffer(kv.persister.ReadSnapshot())
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	kv.mu.Lock()
	
	index := kv.lastIncludedIndex
	term := kv.lastIncludedTerm
	
	if lastIncludedIndex >= index {
		kv.mu.Unlock()
		return index, term, false
	}
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	// e.Encode(kv.Configs)
	e.Encode(kv.ConfigNum)
	e.Encode(kv.Shards)
	e.Encode(kv.executedID)
	e.Encode(kv.data)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
	kv.mu.Unlock()
	return index, term, true
}

func (kv *ShardKV) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	// kv.Shards = make(map[int]int)
	kv.data = make(map[string]string)
	d.Decode(&kv.lastIncludedIndex)
	d.Decode(&kv.lastIncludedTerm)
	// d.Decode(&kv.Configs)
	d.Decode(&kv.ConfigNum)
	d.Decode(&kv.Shards)
	d.Decode(&kv.executedID)
	d.Decode(&kv.data)
}

func (kv * ShardKV) clientRequest(op Op, reply *OpReply) {
	kv.mu.Lock()
	if _, ok := kv.executedID[op.Client]; !ok {
		kv.executedID[op.Client] = -1
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	//**fmt.Println("Client Request:", op, " Server:", kv.me, ", gid:", kv.gid)
	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make([]chan OpReply, 0)
	}
	notifyMe := make(chan OpReply)
	kv.notify[index] = append(kv.notify[index], notifyMe)
	kv.mu.Unlock()
	
	var executedOp OpReply
	var notified = false
	for {
		select {
		case executedOp = <- notifyMe:
			notified = true
			break
		case <- time.After(10*time.Millisecond):
			kv.mu.Lock()
			if currentTerm, _ := kv.rf.GetState(); term != currentTerm {
				if kv.lastIncludedIndex < index {
					reply.WrongLeader = true
					delete(kv.notify, index)
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
		case <- kv.QuitCH:
			reply.Err = "ServerFail"
			//**fmt.Println(" Op ServerFail: ", op)
			return
		}
		if notified {
			break
		}
	}

	if executedOp.Client != op.Client || executedOp.Sequence != op.Sequence {
		reply.Err = "FailCommit"
		reply.WrongLeader = true
		//**fmt.Println("FailCommit:", reply)
	} else {
		reply.Err = executedOp.Err
		reply.WrongLeader = false
		reply.Client = executedOp.Client
		reply.Sequence = executedOp.Sequence
		reply.Value = executedOp.Value
		//**fmt.Println("Request executed: ", reply)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Client:args.Client, Sequence:args.Sequence, Type: "Get", Key: args.Key}
	opreply := &OpReply{}
	kv.clientRequest(op, opreply)
	reply.WrongLeader = opreply.WrongLeader
	reply.Err = opreply.Err
	reply.Value = opreply.Value
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Client:args.Client, Sequence:args.Sequence, Type: args.Op, Key: args.Key, Value: args.Value}
	opreply := &OpReply{}
	kv.clientRequest(op, opreply)
	reply.WrongLeader = opreply.WrongLeader
	reply.Err = opreply.Err
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.QuitCH)
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.data = make(map[string]string)
	kv.executedID = make(map[int64]int64)
	kv.notify = make(map[int][]chan OpReply)
	kv.QuitCH = make(chan bool)
	kv.Msgs = list.New()
	kv.Recv = make(chan raft.ApplyMsg)
	kv.lastIncludedIndex = 0
	kv.lastIncludedTerm = 0
	// kv.Shards = make(map[int]int)
	kv.mck = shardmaster.MakeClerk(masters)
	kv.servers = servers
	kv.Configs = make([]shardmaster.Config, 1)
	kv.Configs[0].Groups = map[int][]string{}
	kv.ConfigNum = 0
	
	kv.readPersist(persister.ReadSnapshot())
	
	if kv.rf.FirstIndex() < kv.lastIncludedIndex {
		kv.rf.CutLog(kv.lastIncludedIndex, kv.lastIncludedTerm)
	} else if kv.rf.FirstIndex() > kv.lastIncludedIndex {
		//**fmt.Println("error 2")
	} else {
		//**fmt.Println(kv.me, "First Index of log: ", kv.rf.FirstIndex())
	}

	if kv.ConfigNum != 0 {
		for i := 1; i <= kv.ConfigNum; i++ {
			cf := kv.mck.Query(i)
			kv.Configs = append(kv.Configs, cf)
		}
	}


	n := len(kv.Configs)
	for key, _ := range kv.Configs[n-1].Shards {
		if kv.Shards[key] != 0 && kv.Shards[key] != kv.Configs[n-1].Num {
			for i := 1; i < len(kv.Configs); i++ {
				if kv.Configs[i].Num >  kv.Shards[key] {
					if kv.Configs[i].Shards[key] != kv.gid {
						args := kv.makeSendShardArgs(key, kv.Configs[i-1], kv.Configs[i])
						go kv.sendShard(args)
						break
					}
				}
			}	
		}
	}
	
	//**fmt.Println("Starting server:", kv.me, ", gid:", kv.gid)
	go func() {
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
				//**fmt.Println("Receive command, Server:", kv.me, ", gid: ", kv.gid, ", Index:", msg.Index)
				kv.mu.Unlock()
			case <- kv.QuitCH:
				return
			}
		}
	} ()

	go func() {
		if maxraftstate <= 0 {
			return
		}
		for {
			select {
			case <- time.After(1 * time.Millisecond):
				if kv.rf.BeginSnapshot() {
					if persister.RaftStateSize() >= maxraftstate*3/5 {
						// //**fmt.Println(kv.me, " log too large")
						index, term, ok := kv.persist()
						if ok {
							kv.rf.CutLog(index, term)
							//**fmt.Println(kv.me, " cut log to ", index, " gid:", kv.gid)
						}
					}
					kv.rf.EndSnapshot()
				}
			case <- kv.QuitCH:
				return
			}
		}
	} ()

	go kv.updateConfig()
	
	return kv
}

func (kv *ShardKV) applyCommand(msg raft.ApplyMsg) {
	if msg.UseSnapshot {
		//**fmt.Println(kv.me, " receive snapshot through channel, gid: ", kv.gid)
		kv.readPersist(msg.Snapshot)
		kv.Configs = make([]shardmaster.Config, 1)
		kv.Configs[0].Groups = map[int][]string{}
		if kv.ConfigNum != 0 {
			for i := 1; i <= kv.ConfigNum; i++ {
				cf := kv.mck.Query(i)
				kv.Configs = append(kv.Configs, cf)
			}
		}
		return
	}
	
	kv.lastIncludedIndex = msg.Index
	kv.lastIncludedTerm = msg.Term
	
	op := msg.Command.(Op)
	logIndex := msg.Index
	clientID := op.Client
	opSequence := op.Sequence
	res := &OpReply{Client: clientID, Sequence: opSequence}

	if _, ok := kv.executedID[clientID]; !ok {
		kv.executedID[clientID] = -1
	}

	if op.Type == "Config" {
		kv.applyConfig(op, res)
	} else if op.Type == "InstallShard" {
		kv.applyInstallShard(op, res)
	} else if kv.gid != kv.Configs[len(kv.Configs)-1].Shards[key2shard(op.Key)] {
		res.Err = ErrWrongGroup
		//**fmt.Println("ErrWrong group, Server:", kv.me, ", gid: ", kv.gid, ", op.Key:", op.Key, ", shard:", key2shard(op.Key), "should be:", kv.Configs[len(kv.Configs)-1].Shards[key2shard(op.Key)])
	} else if kv.Shards[key2shard(op.Key)] < kv.Configs[len(kv.Configs)-1].Num {
		res.Err = "WaitReceive"
		//**fmt.Println("WaitReceive, Server:", kv.me, ", gid: ", kv.gid, ", op.Key:", op.Key, ", shard:", key2shard(op.Key), "config:", kv.Configs[len(kv.Configs)-1].Num, " shard config #:", kv.Shards[key2shard(op.Key)])
	} else if opSequence > kv.executedID[clientID] {
		switch op.Type {
		case "Get":
			kv.applyGet(op, res)
		case "Put":
			kv.applyPut(op, res)
		case "Append":
			kv.applyAppend(op, res)
		}
		kv.executedID[clientID] = opSequence
	} else {
		res.Err = OK
		if op.Type == "Get" {
			v, ok := kv.data[op.Key]
			if !ok {
				res.Err = ErrNoKey
			} else {
				res.Value = v
			}
		}
		//**fmt.Println("Outdated Command: ", op, " and clientid: ", clientID, " kv.Sequence: ", kv.executedID[clientID], "res:", res)
	}

	if _, ok := kv.notify[logIndex]; !ok {
		return
	}

	for _,  c := range kv.notify[logIndex] {
		kv.mu.Unlock()
		c <- *res
		kv.mu.Lock()
	}

	delete(kv.notify, logIndex)
}


func (kv *ShardKV) updateConfig() {
	for {
		select {
		case <- time.After(50*time.Millisecond):
			kv.mu.Lock()
			oldConf := kv.Configs[len(kv.Configs)-1]
			cf := kv.mck.Query(-1)
			if cf.Num > oldConf.Num {
				if cf.Num > oldConf.Num + 1 {
					cf = kv.mck.Query(oldConf.Num+1)
				}
				op := Op{Config:cf, Type: "Config"}
				op.Sequence = nrand()
				opreply := &OpReply{}
				kv.mu.Unlock()
				// //**fmt.Println("Server:", kv.me, ".cf.Num:", cf.Num, ".gid:", kv.gid)
				kv.clientRequest(op, opreply)
				kv.mu.Lock()
				if !opreply.WrongLeader && opreply.Err == OK {
					//**fmt.Println("Server:", kv.me, ".cf.Num:", cf.Num, ".gid:", kv.gid, "Succeed!", "oldConf.Num:", oldConf.Num)
				}
			}
			kv.mu.Unlock()
		case <- kv.QuitCH:
			return
		}
	}
}

func (kv *ShardKV) makeSendShardArgs(s int, config1 shardmaster.Config, config2 shardmaster.Config) SendShardArgs {
	args := SendShardArgs{}
	args.Shard = s
	args.Data = make(map[string]string)
	args.ExecutedID = make(map[int64]int64)
	args.Config = config1
	args.NewConfig = config2
	for key := range kv.data {
		shard := key2shard(key)
		if s == shard {
			args.Data[key] = kv.data[key]
		}
	}
	for client := range kv.executedID {
		args.ExecutedID[client] = kv.executedID[client]
	}
	return args
}

func (kv *ShardKV) sendShard(args SendShardArgs) {
	servers := args.NewConfig.Groups[args.NewConfig.Shards[args.Shard]]
	for {
		select {
		case <- time.After(5*time.Millisecond):
			kv.mu.Lock()
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				reply := SendShardReply{}
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.InstallShard", args, &reply)
				kv.mu.Lock()
				if ok && reply.WrongLeader == false && reply.Err == OK {
					//**fmt.Println("Server:", kv.me, ".gid:", kv.gid, "Send shard:", args.Shard, " to gid: ", args.NewConfig.Shards[args.Shard], " success.")
					// kv.deleteShard(args.Data, args.Shard)
					kv.deleteShard2(args.Data, args.Shard, args.Config.Num)
					kv.mu.Unlock()
					//kv.persist()
					return
				}
			}
			kv.mu.Unlock()
		case <- kv.QuitCH:
			return
		}
//		<- time.After(5*time.Millisecond)
	}
}
	

func (kv *ShardKV) InstallShard(args SendShardArgs, reply *SendShardReply) {
	kv.mu.Lock()
	
	num := kv.Shards[args.Shard]
	if num >= args.Config.Num {
		reply.Err = OK
		//**fmt.Println("Server:", kv.me,". gid:", kv.gid," install shard but outdate, return OK, shard:", args.Shard, "num:", num, "kv.Shards[args.Shard]:", kv.Shards[args.Shard])
		kv.mu.Unlock()
		return
	}

	op := Op{Type: "InstallShard", Data: args.Data, Shard: args.Shard, ExecutedID: args.ExecutedID, Config: args.NewConfig}
	op.Sequence = nrand()
	opreply := &OpReply{}
	kv.mu.Unlock()
	kv.clientRequest(op, opreply)
	kv.mu.Lock()
	reply.WrongLeader = opreply.WrongLeader
	reply.Err = opreply.Err
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) deleteShard2(data map[string]string, s int, num int) {
	//**fmt.Println("Server:", kv.me, " delete shard: ", s)
	if kv.Shards[s] <= num {
		for i := range data {
			if _, ok := kv.data[i]; ok {
				delete(kv.data, i)
				//fmt.Println(kv.me, " at ", kv.gid, " delete:", i)
			}
		}
		//**fmt.Println("Server:", kv.me, " deleted shard: ", s, " and kv.Shards:", kv.Shards[s])
		kv.Shards[s] = 0
		// Because kv.persist() fo not persist when kv.lastIncludedIndex does not change.
		// So do not call the persist() function. Instead directly save the state
		if (kv.maxraftstate > 0) {
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			e.Encode(kv.lastIncludedIndex)
			e.Encode(kv.lastIncludedTerm)
			e.Encode(kv.ConfigNum)
			e.Encode(kv.Shards)
			e.Encode(kv.executedID)
			e.Encode(kv.data)
			data := w.Bytes()
			kv.persister.SaveSnapshot(data)
		}
	}
}

func (kv *ShardKV) applyConfig(op Op, reply *OpReply) {
	if kv.Configs[len(kv.Configs)-1].Num >= op.Config.Num {
		reply.Err = OK
	} else {
		oldConf := kv.Configs[len(kv.Configs)-1]
		kv.Configs = append(kv.Configs, op.Config)
		kv.ConfigNum = op.Config.Num
		//**fmt.Println("Server:", kv.me, " applying config ", op.Config)
		if oldConf.Num == 0 {
			for i, gid := range op.Config.Shards {
				if gid == kv.gid {
					kv.Shards[i] = op.Config.Num
					//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "shard:", i, " to conf#:", op.Config.Num)
				} else {
					kv.Shards[i] = 0
				}
			}
		} else {
			for i, gid := range op.Config.Shards {
				if gid == kv.gid && kv.Shards[i] == oldConf.Num {
					kv.Shards[i] = op.Config.Num
					//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "shard:", i, " to conf#:", op.Config.Num)
				}
			}
		}

		if oldConf.Num != 0 {
			for key, gid := range op.Config.Shards {
				if kv.Shards[key] == oldConf.Num && gid != kv.gid {
					args := kv.makeSendShardArgs(key, oldConf, op.Config)
					//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, " make send shard args:", key, " sending to ", op.Config.Shards[key])
					go kv.sendShard(args)
				}
			}
		}
		
		reply.Err = OK
		//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, " apply config:", op.Config, "kv.Shards:", kv.Shards, " done.")
	}
}

func (kv *ShardKV) applyInstallShard(op Op, reply *OpReply) {
	if op.Config.Num > kv.Configs[len(kv.Configs)-1].Num {
		reply.Err = "Wait"
		//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, " wait to config:", op.Config, "kv.Config:", kv.Configs[len(kv.Configs)-1], "shards:", op.Shard)
	} else {
		reply.Err = OK

		if op.Config.Num > kv.Shards[op.Shard] {
			//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "install shard on config:", op.Config, "kv.Shards[op.Shard]:", kv.Shards[op.Shard], "shards:", op.Shard)
			kv.Shards[op.Shard] = op.Config.Num
			for i, v := range op.Data {
				kv.data[i] = v
			}
			for i, v := range op.ExecutedID {
				if seq, ok := kv.executedID[i]; !ok || seq < v {
					kv.executedID[i] = v
				}
			}

			// send out the shard?
			for i := 0; i < len(kv.Configs); i++ {
				if kv.Configs[i].Num > op.Config.Num {
					if kv.Configs[i].Shards[op.Shard] == kv.gid {
						//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "move installed shard # to config:", kv.Configs[i], "kv.Shards[op.Shard]:", kv.Shards[op.Shard], "shards:", op.Shard)
						kv.Shards[op.Shard] = kv.Configs[i].Num
					} else {
						//**fmt.Println("Server:", kv.me, ", gid: ", kv.gid, "send installed shard # to config:", kv.Configs[i], "kv.Shards[op.Shard]:", kv.Shards[op.Shard], "shards:", op.Shard, " from config:", kv.Configs[i-1])
						args := kv.makeSendShardArgs(op.Shard, kv.Configs[i-1], kv.Configs[i])
						go kv.sendShard(args)
						break
					}
				}
			}
		}
	}
}

func (kv *ShardKV) applyGet(op Op, reply *OpReply) {
	v, ok := kv.data[op.Key]
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Value = v
		reply.Err = OK
	}
	//**fmt.Println("Server:", kv.me, ",gid:", kv.gid, " Get at key ", op.Key, " shard:", key2shard(op.Key))
}

func (kv *ShardKV) applyPut(op Op, reply *OpReply) {
	kv.data[op.Key] = op.Value
	//**fmt.Println("Server:", kv.me, ",gid:", kv.gid, " Put at key ", op.Key, " shard:", key2shard(op.Key))
	reply.Err = OK
}

func (kv *ShardKV) applyAppend(op Op, reply *OpReply) {
	_, ok := kv.data[op.Key]
	if !ok {
		kv.data[op.Key] = op.Value
	} else {
		kv.data[op.Key] += op.Value
	}
	//**fmt.Println("Server:", kv.me, ",gid:", kv.gid, " append at key ", op.Key, " shard:", key2shard(op.Key))
	reply.Err = OK
}
