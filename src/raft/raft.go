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

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"
import "fmt"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type LogEntry struct {
        Index   int
        Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm   int
	VotedFor      int
	Log           []LogEntry
	
	CommitIndex   int
	LastApplied   int

        State         string
	NextIndex     []int
	MatchIndex    []int

        HeartBeatCH   chan bool
	ToFollower    chan bool
	ApplyCH       chan ApplyMsg

}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = (rf.State == "Leader")
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	for i, entry := range rf.Log {
	    if i != 0 {
	        e.Encode(entry)
	    }
	}
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	for {
	    var entry LogEntry
	    err := d.Decode(&entry)
	    if err != nil { break }
	    rf.Log = append(rf.Log, entry)
	}
	rf.mu.Unlock()	   
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
        Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
        ConflictEntry int
        Term          int
	Success       bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	//**//**fmt.Println("Node ", rf.me, " receiving request vote from node ", args.CandidateId)
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
	    reply.VoteGranted = false
	  //$fmtPrintln("Node ", rf.me, "'s Term is larger than candidate ", args.CandidateId, "'s term, deny")
	    rf.mu.Unlock()
	} else {
	    if rf.CurrentTerm < args.Term {
	      //$fmtPrintln(rf.me, " becomes follower from ", rf.State," in receiving voterequest from", args.CandidateId, ". And term increase from", rf.CurrentTerm, " to ", args.Term)
	        rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		rf.mu.Unlock()
//		go rf.persist()
		go func() { rf.ToFollower <- true } ()
		rf.mu.Lock()
	    }
	    reply.Term = rf.CurrentTerm
	    logIndex := len(rf.Log) - 1
	    logTerm := rf.Log[logIndex].Term
	    if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
	        ((logTerm < args.LastLogTerm) || (logTerm == args.LastLogTerm && logIndex <= args.LastLogIndex)) {
		rf.VotedFor = args.CandidateId
		rf.mu.Unlock()
//		go rf.persist()
		go func() { rf.HeartBeatCH <- true } ()
		rf.mu.Lock()
		reply.VoteGranted = true
		//**fmt.Println("Node ", rf.me, " granted ", args.CandidateId)
	    } else {
	        reply.VoteGranted = false
	    }
	    rf.mu.Unlock()
	    go rf.persist()
	}
//	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
//	//**//**fmt.Println(rf.me, " is receiving AppendEntries Call from Node ", args.LeaderId)
        changed := false
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.ConflictEntry = args.PrevLogIndex + 1
	if args.Term < rf.CurrentTerm {
	    reply.Success = false
	  //$fmtPrintln("Receiver Node ", rf.me, "'s term ", rf.CurrentTerm, " is larger than Caller ", args.LeaderId, "'s term ", args.Term)
	} else {
	    go func() { rf.HeartBeatCH <- true } ()
	    if rf.CurrentTerm < args.Term {
	      //$fmtPrintln(rf.me, " becomes follower from ", rf.State," in receiving appendrequest from", args.LeaderId, ". And term increase from", rf.CurrentTerm, " to ", args.Term)
	        rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		changed = true
		rf.mu.Unlock()
		go func() { rf.ToFollower <- true } ()
//		go rf.persist()
		rf.mu.Lock()
	    }
	    reply.Term = rf.CurrentTerm
	    if len(rf.Log) <= args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
	        reply.Success = false
		var conflict int
		if len(rf.Log)-1 < args.PrevLogIndex {
		    conflict = len(rf.Log)
//		    //**//**fmt.Println(conflict, " is len(log)")
		} else {
		    conflict = args.PrevLogIndex
		    var conflictTerm = rf.Log[conflict].Term
		    fi := conflict
		    for ; fi > 0 && rf.Log[fi].Term == conflictTerm; fi-- {}
		    conflict = fi + 1
//		    //**//**fmt.Println(conflict, " is fi + 1")
		}
		reply.ConflictEntry = conflict
//		//**//**fmt.Println(rf.me, " has reply.ConflictEntry: ", reply.ConflictEntry, " and PrevLogIndex: ", args.PrevLogIndex)
//		if len(args.Entries) > 0 {
//		    //**//**fmt.Println("And NextIndex : ", args.Entries[0].Index)
//		}
//		//**//**fmt.Println("Receiver Node ", rf.me, " mismatvh with caller ", args.LeaderId, " log.")
//		//**//**fmt.Println("args.PrevLogIndex: ", args.PrevLogIndex, ". args.PrevLogTerm", args.PrevLogTerm)
//		//**//**fmt.Println("Receiver Node ", rf.me, ". len(rf.Log) = ", len(rf.Log), ". last entry term: ", rf.Log[args.PrevLogIndex].Term)
	    } else {
	        if len(args.Entries) > 0 {
//		    //**//**fmt.Println("Appending entries...")
	            for _, e := range args.Entries {
		        if len(rf.Log)-1 < e.Index {
			    rf.Log = append(rf.Log, e)
			  //$fmtPrintln(rf.me, " append entry ", e.Index, " in log of length ", len(rf.Log))
			} else if rf.Log[e.Index].Term != e.Term {
			    rf.Log = rf.Log[:e.Index]
			    rf.Log = append(rf.Log, e)
			  //$fmtPrintln(rf.me, " append entry ", e.Index, " in log of length ", len(rf.Log))
			}
			changed = true
		    }
//		    rf.mu.Unlock()
//		    go rf.persist()
//		    rf.mu.Lock()
		}
		if args.LeaderCommit > rf.CommitIndex {
		    if args.LeaderCommit > len(rf.Log)-1 {
		        rf.CommitIndex = len(rf.Log)-1
		    } else {
		        rf.CommitIndex = args.LeaderCommit
		    }
		  //$fmtPrintln(rf.me, "'s CommitIndex: ", rf.CommitIndex, " and args.LeaderCommit: ", args.LeaderCommit, " and len(rf.Log)-1:", len(rf.Log)-1)
		    go rf.processApplyChan()
		}
		reply.Success = true
	    }
	    if changed {
	        rf.mu.Unlock()
	        go rf.persist()
	        rf.mu.Lock()
	    }
	}
	rf.mu.Unlock()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
        if rf.NowState() == "Leader" {
            ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	    return ok
	} else {
	    return false
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

        rf.mu.Lock()
	defer rf.mu.Unlock()
        if rf.State != "Leader" {
	    isLeader = false
	} else {
	//if command.(int) != rf.Log[len(rf.Log)-1].Command.(int) {
	  //$fmtPrintln(rf.me, " receiving command ", command.(int), " and append to log index ", len(rf.Log))
	    entry := LogEntry{len(rf.Log), command, rf.CurrentTerm}
	    rf.Log = append(rf.Log, entry)
	    index = len(rf.Log)-1
	    term = rf.CurrentTerm
	    rf.mu.Unlock()
	    go rf.Broadcast()
	    go rf.persist()
	    rf.mu.Lock()
//	    //**//**fmt.Println("Last cont index: ", len(rf.count), " and Last Log index: ", len(rf.Log))
	}
//	else {
//	    term = rf.CurrentTerm
//	    index = len(rf.Log)-1
//	}

	return index, term, isLeader
}

func (rf *Raft) Broadcast() {
    for i := range rf.peers {
//      //**//**fmt.Println("me: ", rf.me, "Sending heartbeat to ", i)
        if i != rf.me {
	    go func(index int) {
		    rf.mu.Lock()
		    if rf.State != "Leader" {
		        rf.mu.Unlock()
		        return
		    }
	            args := AppendEntriesArgs{}
		    args.Term = rf.CurrentTerm
		    args.LeaderId = rf.me
		    args.PrevLogIndex = rf.NextIndex[index]-1
//		    //**//**fmt.Println(index, "'s NextIndex is: ", rf.NextIndex[index])
		    if rf.NextIndex[index] <= 0 || len(rf.Log) < rf.NextIndex[index]-1 {
		      fmt.Println(" NextIndex[index]:", rf.NextIndex[index], ";", "len(rf.log):", len(rf.Log))
		    }
//		    //**//**fmt.Println("i: ", index, " NextIndex[index]:", rf.NextIndex[index], ";", "len(rf.log):", len(rf.Log))
		    args.PrevLogTerm = rf.Log[rf.NextIndex[index]-1].Term
		    args.LeaderCommit = rf.CommitIndex
		    reply := &AppendEntriesReply{}
	            if rf.NextIndex[index] < len(rf.Log) {
		        args.Entries = rf.Log[rf.NextIndex[index]:]
//			//**//**fmt.Println("In args.Entries, first index is: ", args.Entries[0].Index)
		    } else {
		        args.Entries = make([]LogEntry, 0)
		    }
		    rf.mu.Unlock()
		    //**//**fmt.Println(rf.me, "is sending append request to Node ", index, " among ", len(rf.peers), " peers")		  		  
//		    //**//**fmt.Println("Before sending rpc append, node ", index, "'s nextindex is", rf.NextIndex[index])
		    if ok := rf.sendAppendEntries(index, args, reply); ok {
//		        //**//**fmt.Println("Before processing, rf.NextIndex[index] of ", index, " is ",  rf.NextIndex[index])
			rf.processAppendReply(args, reply, index)
//			//**//**fmt.Println("After processing, rf.NextIndex[index] of ", index, " is ",  rf.NextIndex[index])
		    }
	    } (i)
	}
    }
}


func (rf *Raft) NowState() string {
        rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.State
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.State = "Follower"
	rf.HeartBeatCH = make(chan bool)
	rf.ToFollower = make(chan bool)
	rf.ApplyCH = applyCh
	
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{0, 0, 0}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

//      //**//**fmt.Println("Starting Node ", me)
        go func() {
            for {	        
	        switch rf.NowState() {
	        case "Follower":
	            rf.FollowerState()
	        case "Candidate":
	            rf.CandidateState()
	        case "Leader":
	            rf.LeaderState()
	        }
	    }
	}()
//	go rf.processApplyChan()

	return rf
}

func (rf *Raft) FollowerState() {

  //$fmtPrintln(rf.me, " is follower")
    
    electiontimeout := 200
    randomized := electiontimeout + rand.Intn(electiontimeout)
    nexttimeout := time.Duration(randomized)
    t := time.NewTimer(nexttimeout * time.Millisecond)
    for {
        select {
	case <- t.C:
	    rf.mu.Lock()
	    rf.State = "Candidate"
	  //$fmtPrintln(rf.me, " becomes candidate in follower timeout")
	    rf.mu.Unlock()
	    return
	case <- rf.HeartBeatCH:
	    randomized  = electiontimeout + rand.Intn(electiontimeout)
	    nexttimeout = time.Duration(randomized)
	    t.Reset(nexttimeout * time.Millisecond)
	  //$fmtPrintln(rf.me, " Receiving hearbeat, reset as follower")
	case <- rf.ToFollower:
	    //**//**fmt.Println(rf.me, " Receiving tofollower, do nothing")
	    continue
	}
    }
}

func (rf *Raft) CandidateState() {
    rf.mu.Lock()
    rf.CurrentTerm = rf.CurrentTerm + 1
  //$fmtPrintln("Candidate ", rf.me, "Increases term to ", rf.CurrentTerm, " and election")
    totalVotes := 1
    rf.VotedFor = rf.me
//    //**//**fmt.Println(rf.me, " is Candidate")
    rf.mu.Unlock()
    go rf.persist()
    electiontimeout := 200
    randomized  := electiontimeout + rand.Intn(electiontimeout)
    nexttimeout := time.Duration(randomized)
    t := time.NewTimer(nexttimeout * time.Millisecond)

    VotesCollect := make(chan bool)
    args := RequestVoteArgs{rf.CurrentTerm, rf.me, len(rf.Log)-1, rf.Log[len(rf.Log)-1].Term}
    for i := range rf.peers {
//      //**//**fmt.Println("Sending vote to ", i, "; me is ", rf.me, "len(rf.peers):", len(rf.peers))
        if i != rf.me {
            go func(index int) {
	      //$fmtPrintln("Me: ", rf.me, " send request vote to ", index, " in gorountine")
	        var reply = &RequestVoteReply{}
		var ok = false
		t1 := time.Now()
	        for rf.NowState() == "Candidate" && time.Since(t1).Seconds() < 0.2 {
		    var ok1 = false
		    var reply1 = &RequestVoteReply{}
//		    //**//**fmt.Println(rf.me, " is sending request vote to ", index)
		    t := time.NewTimer(10*time.Millisecond)
		    go func() {
	                ok1 = rf.sendRequestVote(index, args, reply1)
		    } ()
		    for j:= 1; (j < 4) && (!ok1); j++ {
		        <- t.C
			t.Reset(10*time.Millisecond)
		    }
		    <- t.C
		    if ok1 {
		        ok = true
			reply.Term = reply1.Term
			reply.VoteGranted = reply1.VoteGranted
			break
		    }
		}
		if !ok {
		    return
		}
		if reply.VoteGranted {
		    VotesCollect <- true
		  //$fmtPrintln(rf.me, " received vote from ", index)
		} else {
		    rf.mu.Lock()
		    if reply.Term > rf.CurrentTerm {
		      //$fmtPrintln(rf.me, " transfer from candidate to follower when receing votereply; term from", rf.CurrentTerm, " to ", reply.Term, " of Node ", index)
		        rf.CurrentTerm = reply.Term
		        rf.VotedFor = -1
			rf.State = "Follower"
			rf.mu.Unlock()
			go rf.persist()
		    	go func() { rf.ToFollower <- true } ()
		    } else {
		      //$fmtPrintln(rf.me, " received deny from ", index)
		        rf.mu.Unlock()
		    }
		}
	    }(i)
	}
    }

    for rf.NowState() == "Candidate" {
        select {
            case <- t.C:
	      //$fmtPrintln(rf.me, " timeout, restart candidate")
	        return
	    case <- VotesCollect:
		totalVotes++
// *		//**//**fmt.Println("Node ", rf.me, " has total votes: ", totalVotes, " among ", len(rf.peers))
		if totalVotes > len(rf.peers)/2 {
		    rf.mu.Lock()
		    rf.State = "Leader" 
		  //$fmtPrintln(rf.me, " becomes leader")
		    rf.mu.Unlock()
		    return
		}
	    case <- rf.ToFollower:
	        return
	    case <- rf.HeartBeatCH:
	        continue
	}
    }
}

func (rf *Raft) LeaderState() {
    rf.mu.Lock()
    rf.NextIndex = make([]int, len(rf.peers))
    rf.MatchIndex = make([]int, len(rf.peers))
    for i := range rf.peers {
        rf.NextIndex[i] = len(rf.Log)
	rf.MatchIndex[i] = 0
    }
  //$fmtPrintln(rf.me, " is leader")
    rf.mu.Unlock()
    
    for i := range rf.peers {
//      //**//**fmt.Println("me: ", rf.me, "Sending heartbeat to ", i)
        if i != rf.me {
	    go func(index int) {
	        for rf.NowState() == "Leader" {
		    rf.mu.Lock()
		    if rf.State != "Leader" {
		        rf.mu.Unlock()
		        return
		    }
	            args := AppendEntriesArgs{}
		    args.Term = rf.CurrentTerm
		    args.LeaderId = rf.me
		    args.PrevLogIndex = rf.NextIndex[index]-1
//		    //**//**fmt.Println(index, "'s NextIndex is: ", rf.NextIndex[index])
		    if rf.NextIndex[index] <= 0 || len(rf.Log) < rf.NextIndex[index]-1 {
		      fmt.Println(" NextIndex[index]:", rf.NextIndex[index], ";", "len(rf.log):", len(rf.Log))
		    }
//		    //**//**fmt.Println("i: ", index, " NextIndex[index]:", rf.NextIndex[index], ";", "len(rf.log):", len(rf.Log))
		    args.PrevLogTerm = rf.Log[rf.NextIndex[index]-1].Term
		    args.LeaderCommit = rf.CommitIndex
		    reply := &AppendEntriesReply{}
	            if rf.NextIndex[index] < len(rf.Log) {
		        args.Entries = rf.Log[rf.NextIndex[index]:]
//			//**//**fmt.Println("In args.Entries, first index is: ", args.Entries[0].Index)
		    } else {
		        args.Entries = make([]LogEntry, 0)
		    }
		    rf.mu.Unlock()
		    //**//**fmt.Println(rf.me, "is sending append request to Node ", index, " among ", len(rf.peers), " peers")		  		  
//		    //**//**fmt.Println("Before sending rpc append, node ", index, "'s nextindex is", rf.NextIndex[index])
		    go func() {
		        if ok := rf.sendAppendEntries(index, args, reply); ok {
//		            //**//**fmt.Println("Before processing, rf.NextIndex[index] of ", index, " is ",  rf.NextIndex[index])
			    rf.processAppendReply(args, reply, index)
//			    //**//**fmt.Println("After processing, rf.NextIndex[index] of ", index, " is ",  rf.NextIndex[index])
		        }
		    } ()
		    time.Sleep(60*time.Millisecond)
		}
	    } (i)
	}
    }

//    go func() {
//        for rf.NowState() == "Leader" {
//	    rf.mu.Lock()
//	    for N := len(rf.Log)-1; N > rf.CommitIndex && rf.Log[N].Term == rf.CurrentTerm; N-- {
//	        ct := 1
//		for i := range rf.peers {
//		    if rf.me != i && rf.MatchIndex[i] >= N {
//		        ct++
//		    }
//		}
//		if ct > len(rf.peers)/2 {
//		    rf.CommitIndex = N
//		    break
//		}
//	    }
//	    rf.mu.Unlock()
//	    go rf.processApplyChan()
//	}
//    } ()

    for rf.NowState() == "Leader" {
        select {
	case <- rf.ToFollower:
	  //$fmtPrintln(rf.me, "Receiving ToFollower")
	    return
	case <- rf.HeartBeatCH:
	    continue
	}
    }
}


func (rf *Raft) checkCommitIndex() {
     	 rf.mu.Lock()
	 if rf.State == "Leader" {
	 for N := len(rf.Log)-1; N > rf.CommitIndex && rf.Log[N].Term == rf.CurrentTerm; N-- {
	     ct := 1
	     for i := range rf.peers {
	         if rf.me != i && rf.MatchIndex[i] >= N {
		     ct++
		 }
	     }
	     if ct > len(rf.peers)/2 {
	         rf.CommitIndex = N
		 //**fmt.Println(rf.me, "'s commit index changed to ", rf.CommitIndex)
		 break
	     }
	}
	}
	rf.mu.Unlock()
	go rf.processApplyChan()
}

func (rf *Raft) processAppendReply(args AppendEntriesArgs, reply *AppendEntriesReply, index int) {
//      //**//**fmt.Println("me ", rf.me, " is sending append request to Node ", index)
	if reply.Success {
	  //$fmtPrintln(rf.me, " send hear beat to ", index, " and success")
	    if len(args.Entries) > 0 {
	        rf.mu.Lock()
		//**fmt.Println("NextIndex of ", index, " increase from ", rf.NextIndex[index], " to ", args.Entries[len(args.Entries)-1].Index + 1)
		if args.Entries[len(args.Entries)-1].Index + 1 > rf.NextIndex[index] {
		    rf.NextIndex[index] = args.Entries[len(args.Entries)-1].Index + 1
		    rf.MatchIndex[index] = rf.NextIndex[index]-1
		}
		if rf.CommitIndex < rf.MatchIndex[index] && rf.State == "Leader" {
		    rf.mu.Unlock()
		    go rf.checkCommitIndex()
		} else {
		    rf.mu.Unlock()
		}
	    }
	} else {
	    rf.mu.Lock()
	    if reply.Term > rf.CurrentTerm {
	      //$fmtPrintln(rf.me, " from leader to follower in receiving appendreply, term from ", rf.CurrentTerm, " to ", reply.Term, " of Node ", index)
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		rf.mu.Unlock()
		go func() { rf.ToFollower <- true } ()
		go rf.persist()
		rf.mu.Lock()
	    } else {
//	        //**//**fmt.Println(index, " fails ConflictEntry ", reply.ConflictEntry)
	      //$fmtPrintln(index, " fails on log match and nextindex from ", rf.NextIndex[index], " to ", reply.ConflictEntry)
                rf.NextIndex[index] = reply.ConflictEntry
	    }
	    rf.mu.Unlock()
	}
}

func (rf *Raft) processApplyChan() {
	rf.mu.Lock()
	for rf.CommitIndex > rf.LastApplied {
	    rf.LastApplied++
	    msg := ApplyMsg{}
	    msg.Index = rf.LastApplied
	    msg.Command = rf.Log[rf.LastApplied].Command
	    rf.ApplyCH <- msg
	  //$fmtPrintln("Node ", rf.me, " send apply message. LogIndex: ", msg.Index, "; Command: ", msg.Command.(int), "; CommitIndex: ", rf.CommitIndex)
	}
	rf.mu.Unlock()
}
