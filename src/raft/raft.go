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
	//	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int64
	currentTerm int64
	votedFor    int

	electionTicker *time.Ticker
	healthTicker   *time.Ticker

	// log[]
}

const (
	votedForNull int = -1
)

const (
	STATE_LEADER int64 = iota
	STATE_CANDIDATE
	STATE_FOLLOWER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	term := atomic.LoadInt64(&rf.currentTerm)
	state := atomic.LoadInt64(&rf.state)
	return int(term), state == STATE_LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	// Term is the term of leader
	Term int
}

type AppendEntriesReply struct {
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTicker.Reset(electionTimeout)
	if rf.currentTerm > int64(args.Term) {
		log.Println("you are not leader")
		reply.Success = false
	}
	rf.currentTerm = int64(args.Term)

	reply.Success = true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.stateIs(STATE_LEADER) || rf.stateIs(STATE_CANDIDATE) {
		reply.VoteGranted = false
		return
	}

	// $5.1
	if args.Term < int(rf.currentTerm) {
		reply.VoteGranted = false
		return
	}

	// $5.2, $5.4
	if rf.votedFor == votedForNull && rf.logIsUpToDateAsCandidate() {
		reply.VoteGranted = true
		reply.Term = int(rf.currentTerm)
		rf.votedFor = args.CandidateID
		return
	}

	if rf.votedFor != votedForNull && rf.votedFor != args.CandidateID {
		// we've already chosen a leader
		reply.VoteGranted = false
		reply.Term = int(rf.currentTerm)
		return
	}
}

func (rf *Raft) logIsUpToDateAsCandidate() bool {
	// FIXME: Finish the implementation
	return true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	return rf.stateIs(STATE_LEADER)
}

func (rf *Raft) stateIs(st int64) bool {
	return st == rf.state
}

var (
	ErrWrongState    = errors.New("wrong state")
	ErrFailedRPCCall = errors.New("failed rpc call")
)

func (rf *Raft) handleHealthcheck() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// send ApplyMsg to all peers
	for srv := range rf.peers {
		// peer.Call()
		args := AppendEntriesArgs{Term: int(rf.currentTerm)}
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(srv, &args, &reply); !ok {
			// this peer may dead
			log.Printf("failed on rf.sendRequestVote to server %v: %v\n", srv, ErrFailedRPCCall)
			continue
		}
	}
	return nil
}

// See raft paper Figure 2: Rules for servers
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf
func (rf *Raft) handleElection() error {
	if !rf.stateIs(STATE_FOLLOWER) {
		return fmt.Errorf("wrong state")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1

	voteCnt := 0
	for srv := range rf.peers {
		// peer.Call()
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		if ok := rf.sendRequestVote(srv, &args, &reply); !ok {
			log.Printf("failed on rf.sendRequestVote to server %v: %v\n", srv, ErrFailedRPCCall)
		}
		if reply.VoteGranted {
			voteCnt++
		}
	}
	if voteCnt > len(rf.peers)/2 {
		rf.state = STATE_LEADER
	}
	return nil
}

var electionTimeout = 1 * time.Second
var healthCheckDuration = 500 * time.Millisecond

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.electionTicker = time.NewTicker(electionTimeout)
	rf.healthTicker = time.NewTicker(healthCheckDuration)

	for rf.killed() == false {
		select {
		case <-rf.electionTicker.C:
			// become candidate
			rf.handleElection()
		case <-rf.healthTicker.C:
			// if it's not leader, break
			if !rf.isLeader() {
				break
			}
			if err := rf.handleHealthcheck(); err != nil {
				log.Printf("failed on rf.handleHealthcheck: %v", err)
			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(30 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = votedForNull

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
