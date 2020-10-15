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
	"sync"
	"sync/atomic"
	"math/rand"
	"../labrpc"
	"time"
	"math"
	"sort"
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
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

const (
	ElectionUpperTimeout  = 300 
	ElectionLowerTimeout  = 150
	HeartbeatTimeout = 80 
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

func genRand(lower int,upper int) int {
	rand.Seed(time.Now().UnixNano())
    return rand.Intn(upper - lower + 1) + lower
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlcok()

	var term int = rf.currentTerm
	var isleader bool = rf.serverStatus == Leader

	return term, isleader
}

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
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		reply.Term = args.Term
		logLength = len(rf.log)
		// When the term of the candidate is the same as the current term.
		// If the current node hasn't vote for anyone, or it has already voted for this node,
		// and the last log's term and index is at least as updated as this node,
		// grant the vote.
		reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && 
							(logLength == 0    || (rf.log[logLength - 1].Term <= args.LastLogTerm && rf.log[logLength - 1].Idx <= args.LastLogIndex)) 
	} else {
		rf.switchRole(Follower)
	}

}

// This function will only be called by other functions that have locks.
// So we won't need to lock here again.
func (rf *Raft) siwtchRole(Role role) {

	if(rf.role == role) return

	rf.role = role

	switch role {
		// Switch to follower:
		// Set voted for to -1, and initiate receiving heartbeats from leader
		case Follower:
			rf.votedFor = -1
		// Switch to candidate:
		// Initiate the process of asking to be a leader
		case Candidate:
			go rf.initElection()
		// Switch to leader:
		// Reset volatile fields useful for leader, and initiate the heartbeat sending process
		case Leader:
			logLength = len(rf.log)
			nextIndex := logLength == 0 ? -1 : rf.log[logLength - 1].Idx

			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = lastLogIndex + 1
			}

			rf.matchIndex = make([]int, len(rf.peers))
			rf.matchIndex[rf.me] = lastLogIndex
			go rf.initLeaderHeartbeat()
	}
}

// Make raft great again! (Lol...)
func (rf *Raft) initElection() {
	for {
		rf.mu.Lock()
		// Before starting election, increase the current term and voted for itself
		rf.currentTerm = rf.currentTerm + 1
		votes := 1

		// If the node has already quit the campaign, return.
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		} 

		for i := 0; i < len(rf.peers); i++ {
			// Skip itself
			if i == rf.me {
				continue
			}

			// Initialize RPC arguments
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}

			logLength = len(rf.log)

			args.Term = rf.currentTerm
			args.CandidateId = rf.me

			// Avoid out of bound visits
			args.LastLogIndex = logLength == 0 ? -1 : rf.log[logLength - 1].Idx
			args.LastLogTerm = logLength == 0 ? -1 : rf.log[logLength - 1].Term

			ok := sendRPC(i, "Raft.RequestVote", &args, &reply);

			if(ok) {
				if reply.Term > rf.currentTerm {
					rf.switchRole(Follower)
					rf.mu.Unlock()
					return
				} else if reply.VoteGranted {
					votes++
					if votes > len(rf.peers) / 2 + 1 {
						rf.switchRole(Leader)
						rf.mu.Unlock()
						return
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(genRand(ElectionLowerTimeout, ElectionUpperTimeout) * time.Millisecond)
	}
}

// This function is used to send heartbeat to all the peer nodes periodically
func (rf *Raft) initLeaderHeartbeat() {
	for {
		rf.mu.Lock()

		// Check if this node is still the leader
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		
		for i := 0; i < len(rf.peers); i++ {

			// Skip itself
			if i == rf.me {
				continue
			}

			// Initialize RPC arguments
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

			// In this heartbeat sending process, we only need these 3 fields
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.Entries = []
			args.LeaderCommit = rf.commitIndex

			ok := sendRPC(i, "Raft.AppendEntries", &args, &reply);

			if(ok) {
				if reply.Term > rf.currentTerm {
					rf.switchRole(Follower)
					rf.mu.Unlock()
					return
				} else if reply.Success {
					// If the peer node accept the heartbeat of the current node.
					// We can know the matchIndex of this peer node.
					rf.matchIndex[i] = reply.CommitLogIndex
				}
			}
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
func (rf *Raft) updateCommitIndex {
	tmp := make([]string, len(rf.matchIndex))

	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	// tmp[len(tmp) / 2] is the value that most of nodes are greater or equal to
	newIndex := tmp[len(tmp) / 2]

	if rf.logs[newIndex].Term == rf.currentTerm {
		rf.commitIndex = newIndex
	}
}

func (rf *Raft) initFollower() {
	for {
		rf.mu.Lock()

		// Check if this node is still the leader
		if rf.role != Follower {
			rf.mu.Unlock()
			return
		}

		if !rf.hasHeartbeat {
			rf.switchRole(Candidate)
			rf.mu.Unlock()
			return
		} 

		rf.hasHeartbeat = false
		rf.mu.Unlock()
		time.Sleep(ElectionUpperTimeout * time.Millisecond)
	}
}

func (rf* Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	} else {
		rf.switchRole(Follower)

		// Sent an empty entries array, so this request is just for heartbeat
		if len(args.Entries) == 0 {
			rf.hasHeartbeat = true
			reply.Term = rf.currentTerm
			reply.Success = true
		}
		// copy from the very beginning
		if PrevLogIndex == -1 {
			Term           int
			Success        int
			CommitLogIndex int
		}
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
func (rf *Raft) sendRPC(server int, funcName string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call(funcName, args, reply)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
