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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

type LogEntry struct {
	Term    int
	Idx     int
	Command interface{}
}

type Role int

const (
	Follower  Role = 1
	Candidate Role = 2
	Leader    Role = 3
)

const (
	ElectionUpperTimeout = 600
	ElectionLowerTimeout = 200
	HeartbeatTimeout     = 80
)

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
	currentTerm      int
	votedFor         int
	log              []LogEntry
	commitIndex      int
	lastAppliedIndex int
	nextIndex        []int
	matchIndex       []int
	role             Role
	hasHeartbeat     bool
}

func genRand(lower int, upper int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(upper-lower+1) + lower
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// This will only be called by functions with outer lock
// func (rf *Raft) sendHeartbeat() {

// 	var term int = rf.currentTerm
// 	var isLeader bool = false

// 	if rf.role != Leader {
// 		return term, isLeader
// 	}

// 	args := AppendEntriesArgs{}

// 	// In this heartbeat sending process, we only need these 3 fields
// 	args.Term = rf.currentTerm
// 	args.LeaderId = rf.me
// 	args.Entries = make([]LogEntry, 0)
// 	args.LeaderCommit = rf.commitIndex
// 	// logLength := len(rf.log)
// 	rf.mu.Unlock()
// 	replies := 0
// 	repliesCh := make(chan bool, len(rf.peers))

// 	for i := 0; i < len(rf.peers); i++ {
// 		// Skip itself
// 		if i == rf.me {
// 			continue
// 		}
// 		go func(ch chan bool, index int) {

// 			reply := AppendEntriesReply{}

// 			DPrintf("Node %d is sending heartbeat to %d", rf.me, index)

// 			ok := rf.sendAppendEntries(index, &args, &reply)

// 			if ok {
// 				DPrintf("Node %d receives response from %d", rf.me, index)
// 				if reply.Term > rf.currentTerm {
// 					rf.mu.Lock()
// 					rf.currentTerm = reply.Term
// 					rf.hasHeartbeat = true
// 					rf.switchRole(Follower)
// 					term = rf.currentTerm
// 					isLeader = false
// 					rf.mu.Unlock()
// 					return
// 				} else {
// 					// If the peer node accept the heartbeat of the current node.
// 					// We can know the matchIndex of this peer node.
// 					// rf.matchIndex[index] = reply.CommitLogIndex
// 				}
// 			} else {
// 				DPrintf("Node %d fail to receive response from %d", rf.me, index)
// 			}
// 			ch <- reply.Success == 1

// 		}(repliesCh, i)
// 	}

// 	for i := 0; i < len(rf.peers)-1; i++ {
// 		r := <-repliesCh
// 		if r == true {
// 			replies++
// 		}
// 	}

// 	DPrintf("Node %d received %d replies", rf.me, replies)

// 	// Not enough nodes think you are the leader
// 	if replies < len(rf.peers)/2 || rf.role != Leader {
// 		rf.mu.Lock()
// 		DPrintf("Node %d not receiving enough replies, becoming a follower", rf.me)
// 		rf.hasHeartbeat = true
// 		rf.switchRole(Follower)
// 		return term, isLeader
// 	}

// 	isLeader = true
// 	return term, isLeader
// }

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
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

// CommitLogTerm and CommitLogIndex are used for leader to know the match index for a node
type AppendEntriesReply struct {
	Term           int
	Success        int
	CommitLogIndex int
	// CommitLogTerm  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Node %d Get request from %d, his term is %d", rf.me, args.CandidateId, args.Term)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		reply.Term = args.Term
		logLength := len(rf.log)
		// When the term of the candidate is the same as the current term.
		// If the current node hasn't vote for anyone, or it has already voted for this node,
		// and the last log's term and index is at least as updated as this node,
		// grant the vote.
		reply.VoteGranted = (rf.votedFor == -1) &&
			(rf.role == Follower) &&
			(logLength == 0 || (rf.log[logLength-1].Term <= args.LastLogTerm && rf.log[logLength-1].Idx <= args.LastLogIndex))
		if reply.VoteGranted {
			DPrintf("Node %d granted request from %d, his term is %d", rf.me, args.CandidateId, args.Term)
			rf.votedFor = args.CandidateId
			rf.hasHeartbeat = true
		} else {
			DPrintf("Node %d denied request from %d, his term is %d", rf.me, args.CandidateId, args.Term)
		}
	} else {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.hasHeartbeat = true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("Node %d granted request from %d, his term is %d", rf.me, args.CandidateId, args.Term)
		rf.switchRole(Follower)
	}

}
func (rf *Raft) boot() {
	rf.mu.Lock()
	rf.hasHeartbeat = true
	rf.switchRole(Follower)
	rf.mu.Unlock()
}

// This function will only be called by other functions that have locks.
// So we won't need to lock here again.
func (rf *Raft) switchRole(role Role) {
	if rf.role == role {
		return
	}
	rf.role = role

	switch role {
	// Switch to follower:
	case Follower:
		go rf.initFollower()
	// Switch to candidate:
	// Initiate the process of asking to be a leader
	case Candidate:
		rf.votedFor = rf.me
		go rf.initElection()
	// Switch to leader:
	// Reset volatile fields useful for leader, and initiate the heartbeat sending process
	case Leader:
		logLength := len(rf.log)
		// nextIndex := logLength == 0 ? -1 : rf.log[logLength - 1].Idx
		nextIndex := logLength
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = nextIndex + 1
		}

		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = nextIndex
		go rf.initLeaderHeartbeat()
	}
}

// Make raft great again! (Lol...)
func (rf *Raft) initElection() {
	DPrintf("Node %d into function initElection()", rf.me)

	for {
		rf.mu.Lock()

		// If the node has already quit the campaign, return.
		if rf.role != Candidate {
			rf.mu.Unlock()
			DPrintf("Node %d exit function initElection()", rf.me)

			return
		}
		// Before starting election, increase the current term and voted for itself
		rf.currentTerm = rf.currentTerm + 1
		votes := 1

		args := RequestVoteArgs{}
		logLength := len(rf.log)

		args.Term = rf.currentTerm
		args.CandidateId = rf.me

		// Avoid out of bound visits
		// args.LastLogIndex = logLength == 0 ? -1 : rf.log[logLength - 1].Idx
		args.LastLogIndex = logLength

		if logLength == 0 {
			args.LastLogTerm = rf.currentTerm
		} else {
			args.LastLogIndex = rf.log[logLength-1].Term
		}

		DPrintf("Node %d is a %d, and it starts an election, its term is %d", rf.me, rf.role, rf.currentTerm)
		rf.mu.Unlock()

		votesCh := make(chan bool, len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			// Skip itself
			if i == rf.me {
				continue
			}
			go func(ch chan bool, index int) {
				// Initialize RPC arguments
				reply := RequestVoteReply{}

				DPrintf("Node %d tries to send a request vote to %d", rf.me, index)

				ok := rf.sendRequestVote(index, &args, &reply)

				if ok {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						DPrintf("Node %d has term %d, but it received higher term %d when elect from %d", rf.me, rf.currentTerm, reply.Term, index)
						rf.currentTerm = reply.Term
						rf.hasHeartbeat = true
						rf.switchRole(Follower)
						rf.mu.Unlock()
						DPrintf("Node %d exit function initElection()", rf.me)
						return
					} else {
						DPrintf("Node %d has term %d, received votes from %d", rf.me, rf.currentTerm, i)
						ch <- reply.VoteGranted
					}
				}
			}(votesCh, i)
		}

		for i := 0; i < len(rf.peers); i++ {
			// Skip itself
			if i == rf.me {
				continue
			}
			r := <-votesCh
			if r == true {
				votes++
			}
		}

		rf.mu.Lock()

		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}

		if votes > len(rf.peers)/2 {
			rf.switchRole(Leader)
			rf.mu.Unlock()
			return
		}

		DPrintf("Node %d not receiving enough votes, end this round", rf.me)
		timeout := time.Duration(genRand(ElectionLowerTimeout, ElectionUpperTimeout))
		time.Sleep(time.Millisecond * timeout)
	}
}

// This function is used to send heartbeat to all the peer nodes periodically
func (rf *Raft) initLeaderHeartbeat() {
	DPrintf("Node %d into function initLeaderHeartbeat()", rf.me)
	for {
		rf.mu.Lock()

		DPrintf("Node %d is a %d, its term is %d, it should be a leader", rf.me, rf.role, rf.currentTerm)

		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{}

		// In this heartbeat sending process, we only need these 3 fields
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.Entries = make([]LogEntry, 0)
		args.LeaderCommit = rf.commitIndex
		// logLength := len(rf.log)
		rf.mu.Unlock()
		replies := 0
		repliesCh := make(chan bool, len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			// Skip itself
			if i == rf.me {
				continue
			}
			go func(ch chan bool, index int) {

				reply := AppendEntriesReply{}

				DPrintf("Node %d is sending heartbeat to %d", rf.me, index)

				ok := rf.sendAppendEntries(index, &args, &reply)

				if ok {
					DPrintf("Node %d receives response from %d", rf.me, index)
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.hasHeartbeat = true
						rf.switchRole(Follower)
						rf.mu.Unlock()
						return
					} else {
						// If the peer node accept the heartbeat of the current node.
						// We can know the matchIndex of this peer node.
						// rf.matchIndex[index] = reply.CommitLogIndex
					}
				} else {
					DPrintf("Node %d fail to receive response from %d", rf.me, index)
				}
				ch <- reply.Success == 1

			}(repliesCh, i)
		}

		for i := 0; i < len(rf.peers)-1; i++ {
			r := <-repliesCh
			if r == true {
				replies++
			}
		}

		DPrintf("Node %d received %d replies", rf.me, replies)
		rf.mu.Lock()
		// Not enough nodes think you are the leader
		if replies < len(rf.peers)/2 || rf.role != Leader {
			DPrintf("Node %d not receiving enough replies, becoming a follower", rf.me)
			rf.hasHeartbeat = true
			rf.switchRole(Follower)
			rf.mu.Unlock()
			return
		}

		rf.updateCommitIndex()
		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout * time.Millisecond)

	}
}

// If most of the peer nodes has matched indexes bigger than a certain index,
// use that maximum index as the current commitIndex of the leader.
// This utility function can only be called by other functions with synchronization mechanisms,
// so we don't need to lock here.
func (rf *Raft) updateCommitIndex() {
	tmp := make([]int, len(rf.matchIndex))

	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	// tmp[len(tmp) / 2] is the value that most of nodes are greater or equal to
	newIndex := tmp[len(tmp)/2]

	if newIndex == 0 || rf.log[newIndex].Term == rf.currentTerm {
		rf.commitIndex = newIndex
	}
}

func (rf *Raft) initFollower() {
	DPrintf("Node %d into function initFollower()", rf.me)

	for {
		rf.mu.Lock()

		// Check if this node is still the follower
		if rf.role != Follower {
			rf.mu.Unlock()
			DPrintf("Node %d exit function initFollower()", rf.me)

			return
		}

		DPrintf("Node %d is a %d, its term is %d, it should be a follower", rf.me, rf.role, rf.currentTerm)

		if !rf.hasHeartbeat {
			rf.switchRole(Candidate)
			rf.mu.Unlock()
			DPrintf("Node %d exit function initFollower()", rf.me)
			return
		}

		rf.hasHeartbeat = false
		rf.mu.Unlock()
		timeout := time.Duration(genRand(ElectionLowerTimeout, ElectionUpperTimeout))
		time.Sleep(time.Millisecond * timeout)
		// time.Sleep(ElectionUpperTimeout * time.Millisecond)
	}
}

// Stopped here for 10/14
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = 0
		return
	} else {
		rf.hasHeartbeat = true
		rf.currentTerm = args.Term
		rf.switchRole(Follower)
		// Sent an empty entries array, so this request is just for heartbeat
		if len(args.Entries) == 0 {
			DPrintf("Node %d receives heartbeat from server %d", rf.me, args.LeaderId)
			reply.Term = rf.currentTerm
			reply.Success = 1
			reply.CommitLogIndex = rf.commitIndex
			return
		}
		// // copy from the very beginning
		// if PrevLogIndex == -1 {

		// }
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

// A more universal version compared to the example code
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.hasHeartbeat = false
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.boot()
	return rf
}
