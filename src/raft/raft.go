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

// Entry structure in log
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
	
	// Persist on all ervers
	CurrentTerm   int
	VotedFor      int
	Log           []LogEntry
	
	//Volatile state on all servers
	CommitIndex   int
	LastApplied   int
        State         string
	
	// Volatile state on leaders
	NextIndex     []int
	MatchIndex    []int

        HeartBeatCH   chan bool
	ToFollower    chan bool
	ApplyCH       chan ApplyMsg
	QuitCH        chan bool
	active        bool
	
	Snapshotting      []bool                   // whether a leader is sending snapshot to a follower
	tmpSnapshot       map[int]*SnapshotInfo    // last included index to snapshot, for receiver(follower)
	sendingSnapshot   map[int]*SnapshotInfo    // peer index to snapshot, for leader
	SnapshotProgress  bool                     // whether a server is taking snapshot, either by kvraft or InstallSnapshot RPC

}

type SnapshotInfo struct {
	data              []byte
	lastIncludedIndex int
	lastIncludedTerm  int
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
	for _, entry := range rf.Log {
	    e.Encode(entry)
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
	firstIndex := true
	for {
		var entry LogEntry
		err := d.Decode(&entry)
		if err != nil { break }
		if firstIndex {
			rf.Log = make([]LogEntry, 0)
			firstIndex = false
		}
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

type InstallSnapshotArgs struct {
     	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
     	Term    int
	Success bool
}
//
// Example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
        defer rf.persist()
	// Your code here.
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	
	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}
	
	// Update CurrentTerm if receiving higher term, update votedFor, and transfer to follower
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		go func() { <- rf.ToFollower} ()
	}

	// if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	reply.Term = rf.CurrentTerm
	logIndex := rf.Log[len(rf.Log) - 1].Index
	logTerm := rf.Log[len(rf.Log) - 1].Term
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		((logTerm < args.LastLogTerm) || (logTerm == args.LastLogTerm && logIndex <= args.LastLogIndex)) {
		rf.VotedFor = args.CandidateId
		go func() { <- rf.HeartBeatCH } ()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	// rf.persist()
	rf.mu.Unlock()
	// go rf.persist()
	
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
        changed := false
	
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.ConflictEntry = args.PrevLogIndex + 1
	
	// reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	go func() { <- rf.HeartBeatCH } ()
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		changed = true
		go func() { <- rf.ToFollower} ()
	}
	
	reply.Term = rf.CurrentTerm
	base := rf.Log[0].Index
	if base > args.PrevLogIndex || base + len(rf.Log) <= args.PrevLogIndex || rf.Log[args.PrevLogIndex-base].Term != args.PrevLogTerm {
		reply.Success = false
		var conflict int
		if base > args.PrevLogIndex {
			conflict = base + 1
		} else if base + len(rf.Log) <= args.PrevLogIndex {
			conflict = base + len(rf.Log)
		} else {
			conflict = args.PrevLogIndex
			var conflictTerm = rf.Log[conflict-base].Term
			fi := conflict
			for ; fi > base && rf.Log[fi-base].Term == conflictTerm; fi-- {}
			conflict = fi + 1
		}
		rf.mu.Unlock()
		reply.ConflictEntry = conflict
	} else {
		newLastIndex := args.PrevLogIndex
		if len(args.Entries) > 0 {
			newLastIndex = args.Entries[len(args.Entries)-1].Index
			for _, e := range args.Entries {
				if base + len(rf.Log)-1 < e.Index {
					rf.Log = append(rf.Log, e)
				} else if rf.Log[e.Index-base].Term != e.Term {
					rf.Log = rf.Log[:e.Index-base]
					rf.Log = append(rf.Log, e)
				}
				// fmt.Println(rf.me, " is appending at ", e.Index)
				changed = true
			}
			// fmt.Println(rf.me, "'s last log index is ", args.Entries[len(args.Entries)-1].Index, " and length is: ", len(rf.Log))
		}
		if args.LeaderCommit > rf.CommitIndex {
			if args.LeaderCommit > newLastIndex {
				if newLastIndex > rf.CommitIndex {
					rf.CommitIndex = newLastIndex
				}
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
			// fmt.Println(rf.me, "'s commit index is ", rf.CommitIndex)
			go rf.processApplyChan()
		}
		rf.mu.Unlock()
		reply.Success = true
	}
	if changed {
		// go rf.persist()
		// rf.mu.Lock()
		rf.persist()
		// rf.mu.Unlock()
	}
}


func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Println(rf.me, "receive install rpc")
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.Success = true
	if rf.CurrentTerm > args.Term {
		rf.mu.Unlock()
		return
	}
	
	go func() { <- rf.HeartBeatCH } ()
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		go func() { <- rf.ToFollower} ()
		reply.Term = rf.CurrentTerm
	}
	
	base := rf.Log[0].Index
	if args.LastIncludedIndex <= base ||
		(args.LastIncludedIndex <= rf.Log[len(rf.Log)-1].Index && rf.Log[args.LastIncludedIndex-base].Term == args.LastIncludedTerm) {
		rf.mu.Unlock()
		return
	}

	if tmp, ok := rf.tmpSnapshot[args.LastIncludedIndex]; ok {
		if len(tmp.data) >= args.Offset {
			// fmt.Println(rf.me, " receving snapshot")
			rf.tmpSnapshot[args.LastIncludedIndex].data = append(rf.tmpSnapshot[args.LastIncludedIndex].data[: args.Offset], args.Data...)
		} else {
			reply.Success = false
			rf.mu.Unlock()
			return
		}
	} else {
		if args.Offset == 0 {
			// fmt.Println(rf.me, " create snapshot file")
			rf.tmpSnapshot[args.LastIncludedIndex] = &SnapshotInfo{args.Data, args.LastIncludedIndex, args.LastIncludedTerm}
		} else {
			// follower crash before being sent all chunks?
			rf.mu.Unlock()
			reply.Success = false
			return
		}
	}
	
	if args.Done {
		// fmt.Println(rf.me, "receives all snapshot, save it.")
		rf.mu.Unlock()
		for !rf.BeginSnapshot() {
			<- time.After(2*time.Millisecond)
		}
		rf.mu.Lock()
		rf.persister.SaveSnapshot(rf.tmpSnapshot[args.LastIncludedIndex].data)
		for key := range rf.tmpSnapshot {
			if key < args.LastIncludedIndex {
				delete(rf.tmpSnapshot, key)
			}
		}
		entry := LogEntry{args.LastIncludedIndex, 0, args.LastIncludedTerm}
		if args.LastIncludedIndex < rf.Log[len(rf.Log)-1].Index {
			rf.Log = append([]LogEntry{entry}, rf.Log[args.LastIncludedIndex+1-base:]...)
		} else {
			rf.Log = []LogEntry{entry}
		}
		// fmt.Println(rf.me, "'s LastAplied change from ", rf.LastApplied, " to ", args.LastIncludedIndex, " because of install rpc")
		if rf.LastApplied > rf.Log[0].Index {
			fmt.Println(rf.me, "'s lastapplied is ", rf.LastApplied, " and base is ", rf.Log[0].Index, " and state is", rf.State)
		}
		rf.LastApplied = args.LastIncludedIndex
		rf.CommitIndex = args.LastIncludedIndex
		rf.mu.Unlock()
		rf.persist()
		rf.EndSnapshot()
		msg := ApplyMsg{args.LastIncludedIndex, 0, true, rf.tmpSnapshot[args.LastIncludedIndex].data}
		go func() { rf.ApplyCH <- msg } ()
	} else {
		rf.mu.Unlock()
	}
	return
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf* Raft) sendSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply)  bool {
	//fmt.Println("sending... to ", rf.me)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	if rf.State != "Leader" {
		rf.mu.Unlock()
		isLeader = false
	} else {
		base := rf.Log[0].Index
		entry := LogEntry{base + len(rf.Log), command, rf.CurrentTerm}
		rf.Log = append(rf.Log, entry)
		index = base + len(rf.Log)-1
		term = rf.CurrentTerm
		go rf.Broadcast()
		rf.mu.Unlock()
		rf.persist()
		// go rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) Broadcast() {
	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				rf.mu.Lock()
				if rf.Snapshotting[index] || rf.NextIndex[index] <= rf.Log[0].Index {
					rf.mu.Unlock()
					return
				}
				args, isleader := rf.makeAppendEntriesArgs(index)
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				if isleader {
					if ok := rf.sendAppendEntries(index, args, reply); ok {
						rf.processAppendReply(args, reply, index)
					}
				}
			} (i)
		}
	}
}

func (rf *Raft) makeAppendEntriesArgs(index int) (AppendEntriesArgs, bool) {
	args := AppendEntriesArgs{}
	isleader := false
	
	if rf.State != "Leader" {
		return args, isleader
	}
	
	isleader = true
	base := rf.Log[0].Index
        args.Term = rf.CurrentTerm
        args.LeaderId = rf.me
        args.PrevLogIndex = rf.NextIndex[index]-1
        if rf.NextIndex[index] <= base || base + len(rf.Log) < rf.NextIndex[index] {
		fmt.Println(" NextIndex[index]:", rf.NextIndex[index], ";", "len(rf.log):", len(rf.Log))
	}
	args.PrevLogTerm = rf.Log[rf.NextIndex[index]-1-base].Term
	args.LeaderCommit = rf.CommitIndex

	// if no more entries to be sent, then send it as heartbeat.
	if rf.NextIndex[index] < base+len(rf.Log) {
		args.Entries = rf.Log[rf.NextIndex[index]-base:]
	} else {
		args.Entries = make([]LogEntry, 0)
	}

	return args, isleader
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
	close(rf.QuitCH)
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
	rf.QuitCH = make(chan bool)
	
	// Persistent State
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{0, 0, 0}
	rf.active = true
	rf.Snapshotting = make([]bool, len(rf.peers))
	rf.tmpSnapshot = make(map[int]*SnapshotInfo)
	rf.sendingSnapshot = make(map[int]*SnapshotInfo)
	rf.SnapshotProgress = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.LastApplied = rf.Log[0].Index
	rf.CommitIndex = rf.Log[0].Index
	go func() {
		for rf.active {
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

	return rf
}

func (rf *Raft) FollowerState() {
	if !rf.active {
		return
	}

	electiontimeout := 200
	randomized := electiontimeout + rand.Intn(electiontimeout)
	nexttimeout := time.Duration(randomized)
	t := time.NewTimer(nexttimeout * time.Millisecond)
	for {
		select {
		case <- t.C:
			rf.mu.Lock()
			rf.State = "Candidate"
			rf.mu.Unlock()
			return
		case rf.HeartBeatCH <- true:
			randomized  = electiontimeout + rand.Intn(electiontimeout)
			nexttimeout = time.Duration(randomized)
			t.Reset(nexttimeout * time.Millisecond)
		case rf.ToFollower <- true:
			continue
		case <- rf.QuitCH:
			rf.mu.Lock()
			rf.active = false
			close(rf.HeartBeatCH)
			close(rf.ToFollower)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) CandidateState() {
	if !rf.active {
		return
	}
	rf.mu.Lock()
	// increase currentTerm, and vote for self
	rf.CurrentTerm = rf.CurrentTerm + 1
	totalVotes := 1
	rf.VotedFor = rf.me
	rf.mu.Unlock()
    
	rf.persist()
	// go rf.persist()
    
	electiontimeout := 200
	randomized  := electiontimeout + rand.Intn(electiontimeout)
	nexttimeout := time.Duration(randomized)
	t := time.NewTimer(nexttimeout * time.Millisecond)

	VotesCollect := make(chan bool)
	rf.mu.Lock()
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				var reply = &RequestVoteReply{}
				var ok = false
				t1 := time.Now()
				for rf.NowState() == "Candidate" && time.Since(t1).Seconds() < 0.2 && rf.CurrentTerm == args.Term {
					// check RPC call every 10 millisecond. If not successful after 200 millisecond, then give up.
					// Candidate may restart election and timer.
					var ok1 = false
					var reply1 = &RequestVoteReply{}
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
				// RPC might fail, election timeout and may re-elect.
				if !ok {
					return
				}
				if reply.VoteGranted {
					<- VotesCollect
				} else {
					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						// if some server has higher term, then update currentTerm, changes to follower, and save state to persist.
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.State = "Follower"
						rf.mu.Unlock()
						// go rf.persist()
						go func() { <- rf.ToFollower } ()
						rf.persist()
					} else {
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
	
	for rf.NowState() == "Candidate" {
		select {
		case <- t.C:
			return
		case VotesCollect <- true:
			totalVotes++
			if totalVotes > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.State = "Leader"
				close(VotesCollect)
				rf.mu.Unlock()
				return
			}
		case rf.ToFollower <- true:
			return
		case rf.HeartBeatCH <- true:
			continue
		case <- rf.QuitCH:
			rf.mu.Lock()
			rf.active = false
			close(rf.HeartBeatCH)
			close(rf.ToFollower)
			close(VotesCollect)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) LeaderState() {
	if !rf.active {
		return
	}
	rf.mu.Lock()
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = rf.Log[len(rf.Log)-1].Index+1
		rf.MatchIndex[i] = rf.Log[0].Index
	}
	currentTerm := rf.CurrentTerm
	rf.Snapshotting = make([]bool, len(rf.peers))
	rf.sendingSnapshot = make(map[int]*SnapshotInfo)
	rf.mu.Unlock()
	// fmt.Println(rf.me, " becomes leader")
	//  rf.Broadcast()
	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				timeout := time.Millisecond
				offset := 0
				done := false
				for rf.NowState() == "Leader" && rf.CurrentTerm == currentTerm {
					// if leader transform to follower while entering makeAppendEntriesArgs function,
					// then do not send that request and becomes to follower
					select {
					case <- time.After(timeout):
						timeout = 60 * time.Millisecond
						rf.mu.Lock()
						if rf.Snapshotting[index] || rf.NextIndex[index] <= rf.Log[0].Index {
							if !rf.Snapshotting[index] {
								rf.Snapshotting[index] = true
								rf.sendingSnapshot[index] = &SnapshotInfo{}
								rf.sendingSnapshot[index].data = rf.persister.ReadSnapshot()
								r := bytes.NewBuffer(rf.sendingSnapshot[index].data)
								d := gob.NewDecoder(r)
								d.Decode(&rf.sendingSnapshot[index].lastIncludedIndex)
								d.Decode(&rf.sendingSnapshot[index].lastIncludedTerm)
								offset = 0
								// fmt.Println(rf.me, " make argument of install snapshot of ", index)
							}
							var dataLen int
							if len(rf.sendingSnapshot[index].data) - offset > 1000 {
								dataLen = 100
								done = false
							} else {
								dataLen = len(rf.sendingSnapshot[index].data) - offset
								done = true
							}
							data := make([]byte, dataLen)
							copy(data, rf.sendingSnapshot[index].data[offset:offset+dataLen])
							args := InstallSnapshotArgs{}
							args.Term = rf.CurrentTerm
							args.LeaderId = rf.me
							args.LastIncludedIndex = rf.sendingSnapshot[index].lastIncludedIndex
							args.LastIncludedTerm = rf.sendingSnapshot[index].lastIncludedTerm
							args.Offset = offset
							args.Data = data
							args.Done = done
							reply := &InstallSnapshotReply{}
							rf.mu.Unlock()
							go func() {
								//fmt.Println(rf.me, " before send snapshot to ", index)
								if ok := rf.sendSnapshot(index, args, reply); ok {
									// fmt.Println(rf.me, " has sent snapshot to ", index)
									rf.processSnapshotReply(args, reply, index, &offset)
								}
							} ()
						} else {
							args, isleader := rf.makeAppendEntriesArgs(index)
							rf.mu.Unlock()
							reply := &AppendEntriesReply{}
							if !isleader {
								return
							}
							go func() {
								if ok := rf.sendAppendEntries(index, args, reply); ok {
									rf.processAppendReply(args, reply, index)
								}
							} ()
						}
						//time.Sleep(60*time.Millisecond)
					case <- rf.QuitCH:
						return
					}
				}
			} (i)
		}
	}
	
	for rf.NowState() == "Leader" {
		select {
		case rf.ToFollower <- true:
			return
		case rf.HeartBeatCH <- true:
			continue
		case <- rf.QuitCH:
			rf.mu.Lock()
			rf.active = false
			close(rf.HeartBeatCH)
			close(rf.ToFollower)
			rf.mu.Unlock()
			return
		}
	}
}


func (rf *Raft) checkCommitIndex() {
	rf.mu.Lock()
	if rf.State == "Leader" {
		for N := len(rf.Log)-1; (rf.Log[N].Index > rf.CommitIndex) && (rf.Log[N].Term == rf.CurrentTerm); N-- {
			ct := 1
			for i := range rf.peers {
				if rf.me != i && rf.MatchIndex[i] >= rf.Log[N].Index {
					ct++
				}
			}
			if ct > len(rf.peers)/2 {
				rf.CommitIndex = rf.Log[N].Index
				// fmt.Println("CommitIndex becomes: ", rf.Log[N].Index)
				// for i := range rf.peers {
				// fmt.Printf("%d", rf.MatchIndex[i])
				// }
				// fmt.Printf("\n")
				break
			}
		}
	}
	rf.mu.Unlock()
	// after updating commit index, apply the new committed entries.
	go rf.processApplyChan()
}

// after receiving from AppendEntries reply, leader should update itself according to reply message
func (rf *Raft) processAppendReply(args AppendEntriesArgs, reply *AppendEntriesReply, index int) {
	
	rf.mu.Lock()
	if reply.Success && args.Term == rf.CurrentTerm  && rf.State == "Leader" {
		// if successful, update nextindex and matchindex for follower.
		// Because replies may be received out of order, so that nextindex should be guaranted not to decrease.
		// After modifying nextindex and matchindex, refresh leader's commitindex.
		if len(args.Entries) > 0 {
			if args.Entries[len(args.Entries)-1].Index + 1 > rf.NextIndex[index] {
				rf.NextIndex[index] = args.Entries[len(args.Entries)-1].Index + 1
				rf.MatchIndex[index] = rf.NextIndex[index]-1
				// fmt.Println(index, "'s netindex is ", rf.NextIndex[index])
			}
			if rf.CommitIndex < rf.MatchIndex[index] {
				go rf.checkCommitIndex()
			}
		}
		rf.mu.Unlock()
	} else {
		// update currentTerm according to follower's term, and might transfer to follower.
		// or update nextindex according to conflictentry sent by follower.
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.State = "Follower"
			rf.mu.Unlock()
			go func() { <- rf.ToFollower} ()
			// go rf.persist()
			rf.persist()
			rf.mu.Lock()
		}

		if rf.State == "Leader" && args.Term == rf.CurrentTerm && args.Term >= reply.Term {
			rf.NextIndex[index] = reply.ConflictEntry
			// fmt.Println(index, "'s netindex is ", rf.NextIndex[index])
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) processSnapshotReply(args InstallSnapshotArgs, reply *InstallSnapshotReply, index int, offset *int) {
	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.State = "Follower"
		rf.mu.Unlock()
		go func() { <- rf.ToFollower} ()
		// go rf.persist()
		rf.persist()
		return
	}
	// process reply only if it is still leader of the term, and it is not an outdated reply
	if rf.State == "Leader" && args.Term == rf.CurrentTerm &&
		rf.Snapshotting[index] && rf.sendingSnapshot[index].lastIncludedIndex == args.LastIncludedIndex && *offset == args.Offset {
		if !reply.Success {
			rf.Snapshotting[index] = false
			*offset = 0
		} else {
			if args.Done {
				*offset = 0
				rf.Snapshotting[index] = false
				rf.NextIndex[index] = args.LastIncludedIndex + 1
			} else {
				*offset = *offset + len(args.Data)
			}
		}
	}
	rf.mu.Unlock()
}

// Put apply message in channel
func (rf *Raft) processApplyChan() {
	rf.mu.Lock()
	for rf.CommitIndex > rf.LastApplied {
		rf.LastApplied++
		base := rf.Log[0].Index
		msg := ApplyMsg{}
		msg.Index = rf.LastApplied
		if rf.LastApplied < base {
			fmt.Println(rf.me, "'s lastapplied is ", rf.LastApplied, " and base is ", base, " and state is", rf.State)
		}
		msg.Command = rf.Log[rf.LastApplied-base].Command
		msg.UseSnapshot = false
		// fmt.Println(rf.me, " send msg ", msg.Index," to channel")
		rf.ApplyCH <- msg
	}
	rf.mu.Unlock()
}

func (rf *Raft) GetTerm(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base := rf.Log[0].Index
	return rf.Log[index - base].Term
}

func (rf *Raft) CutLog(index int, term int) {
	rf.mu.Lock()
	base := rf.Log[0].Index
	entry := LogEntry{index, 0, term}
	if index <= rf.Log[0].Index {
		rf.mu.Unlock()
		fmt.Println("error 3")
		// fmt.Println(rf.me, " is called CutLog, but base:", base, " and index: ", index)
		return
	} else if index <= rf.Log[len(rf.Log)-1].Index {
		rf.Log = rf.Log[index - base + 1:]
		rf.Log = append([]LogEntry{entry}, rf.Log...)
		if rf.LastApplied < rf.Log[0].Index {
			rf.LastApplied = rf.Log[0].Index
			rf.CommitIndex = rf.Log[0].Index
			// fmt.Println(rf.me, "'s lastapplied is ", rf.LastApplied, " and base is ", rf.Log[0].Index, " and state is", rf.State, " because of cut log")
		}
		// fmt.Println("base of ", rf.me, " is cut to ", rf.Log[0].Index)
	} else {
		rf.Log = []LogEntry{entry}
		//fmt.Println("base of ", rf.me, " is cut to ", rf.Log[0].Index, " and applied index changed to ", index)
		fmt.Println("error 1")
		rf.LastApplied = index
	}
	// rf.LastApplied = index
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) FirstIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Log[0].Index
}

func (rf *Raft) BeginSnapshot() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.SnapshotProgress {
		return false
	} else {
		rf.SnapshotProgress = true
		return true
	}
}

func (rf *Raft) EndSnapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SnapshotProgress = false
}
