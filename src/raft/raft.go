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
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"

	"github.com/rs/zerolog/log"
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
	state       int
	currentTerm int
	// when increasing term, we need to set votedFor to proper raft srvID.
	votedFor int

	electionTimer *time.Timer
	healthTicker  *time.Ticker

	// log[]
}

const (
	votedForNull int = -1
	termInit     int = 0
)

const (
	STATE_FOLLOWER int = iota
	STATE_CANDIDATE
	STATE_LEADER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return int(rf.currentTerm), rf.state == STATE_LEADER
}

func (rf *Raft) stateIs(state int) bool {
	return rf.state == state
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

func (rf *Raft) LogInfo(format string, args ...interface{}) {
	log.Info().Msgf(fmt.Sprintf("Raft[%v]state[%v]term(%v): ", rf.me, rf.state, rf.currentTerm)+format, args...)
}

func (rf *Raft) LogError(format string, args ...interface{}) {
	log.Error().Msgf(fmt.Sprintf("Raft[%v]state[%v]term(%v): ", rf.me, rf.state, rf.currentTerm)+format, args...)
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
	LeaderID int
	// Term is the term of leader
	Term int
}

type AppendEntriesReply struct {
	// Term is the current term of follower
	Term    int
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

	rf.LogInfo("handle AppendEntries request for leader: %v, Term: %v", args.LeaderID, args.Term)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state, rf.currentTerm = STATE_FOLLOWER, args.Term
		rf.votedFor = votedForNull
	}

	rf.electionTimer.Reset(genElectionTimeout())

	reply.Success, reply.Term = true, rf.currentTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		rf.LogInfo("done handling RequestVote from Raft[%v]term[%v]: granted: %v", args.CandidateID, args.Term, reply.VoteGranted)
	}()

	rf.LogInfo("in RequestVote for req from srv: %v", args.CandidateID)

	// $5.1
	// request source srv is outdated, reject the vote.
	if args.Term < int(rf.currentTerm) {
		// we've already chosen a leader
		reply.VoteGranted, reply.Term = false, rf.currentTerm
		return
	}

	// From figure 4
	if args.Term > rf.currentTerm && rf.logIsUpToDateAsCandidate() {
		// we are out-of-date, return to follower, and grant vote
		// if we don't grant vote at here, we won't pass TestManyElections2A
		// because candidate only sends 1 RequestVote to us
		rf.state = STATE_FOLLOWER
		rf.currentTerm, rf.votedFor = args.Term, args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// $5.2, $5.4
	// at here, args.Term == rf.currentTerm
	if (rf.votedFor == votedForNull || rf.votedFor == args.CandidateID) && rf.logIsUpToDateAsCandidate() {
		rf.LogInfo("voted for %v", args.CandidateID)
		reply.VoteGranted, reply.Term = true, rf.currentTerm

		rf.votedFor = args.CandidateID
		rf.electionTimer.Reset(genElectionTimeout())
		rf.LogInfo("my currentTerm: %v", rf.currentTerm)
		rf.state = STATE_FOLLOWER

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
	_, isLeader := rf.GetState()
	return isLeader
}

var (
	ErrWrongState    = errors.New("wrong state")
	ErrFailedRPCCall = errors.New("failed rpc call")
)

func (rf *Raft) handleHealthcheck(needLock bool) error {
	if needLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	rf.LogInfo("in handleHealthcheck")
	rf.electionTimer.Reset(foreverTimeout)
	me := rf.me
	currentTerm := rf.currentTerm

	for srvID := range rf.peers {
		if srvID == rf.me {
			continue
		}
		go func(srvID int, me int) {
			args := AppendEntriesArgs{LeaderID: me, Term: currentTerm}
			reply := AppendEntriesReply{}
			if !rf.sendAppendEntries(srvID, &args, &reply) {
				// this peer may dead
				return
			}
			if !reply.Success {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.LogInfo("healthcheck failed: got term: %v", reply.Term)
				if rf.currentTerm < reply.Term {
					// we are outdated, become follower
					rf.state = STATE_FOLLOWER
					rf.currentTerm, rf.votedFor = reply.Term, votedForNull
					rf.electionTimer.Reset(genElectionTimeout())
					return
				}
			}
		}(srvID, me)
	}

	rf.LogInfo("Done handleHealthcheck")

	rf.electionTimer.Reset(genElectionTimeout())

	return nil
}

// See raft paper Figure 2: Rules for servers
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf
// increment rf.currentTerm
// reset election timer
// send RequestVote RPC
// if electionTimer times up, start new election
func (rf *Raft) handleElection() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.electionTimer.Reset(genElectionTimeout())

	rf.state = STATE_CANDIDATE
	rf.currentTerm += 1

	rf.LogInfo("in handleElection")

	// vote for themselves
	rf.votedFor = rf.me
	var voteCnt int64 = 1
	majority := len(rf.peers) / 2

	currentTerm := rf.currentTerm
	for srvID := range rf.peers {
		if srvID == rf.me {
			continue
		}
		go func(srvID int) {
			args := RequestVoteArgs{Term: currentTerm, CandidateID: rf.me}
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(srvID, &args, &reply); !ok {
				return
			}
			// if we are set to leader by other goroutine, just skip it
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.LogInfo("done sending RequestVote to Raft[%v], vote granted: %v", srvID, reply.VoteGranted)
			if rf.stateIs(STATE_CANDIDATE) {
				if reply.Term > rf.currentTerm && currentTerm == rf.currentTerm {
					// FIXME: is this statement in paper ?
					// votee's term is greater them us, we are not leader
					rf.currentTerm, rf.votedFor = reply.Term, votedForNull
					rf.state = STATE_FOLLOWER
					rf.electionTimer.Reset(genElectionTimeout())
					return
				}
				if reply.VoteGranted {
					voteCnt++
					rf.LogInfo("got %v vote", voteCnt)
					if voteCnt > int64(majority) && rf.currentTerm == currentTerm {
						rf.LogInfo("win the election, voteCnt: %v", voteCnt)
						rf.state = STATE_LEADER
						rf.handleHealthcheck(false)
						rf.electionTimer.Reset(genElectionTimeout())
					}
				}
			}

		}(srvID)
	}
	return nil
}

func genElectionTimeout() time.Duration {
	// return getRand() + 400*time.Millisecond
	maxms := big.NewInt(400)
	ms, _ := crand.Int(crand.Reader, maxms)
	return 300*time.Millisecond + time.Duration(ms.Int64())*time.Millisecond
}

var foreverTimeout = 100 * time.Minute

var healthCheckDuration = 150 * time.Millisecond

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// use random timeout to prevent all raft instance to become candidate in the smae time
	// sleep more so only 1 srv will be in candidate state
	// rf.LogInfo("duration: %v", duration)
	rf.mu.Lock()
	rf.electionTimer = time.NewTimer(genElectionTimeout())
	rf.healthTicker = time.NewTicker(healthCheckDuration)
	rf.mu.Unlock()

	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.handleElection()
		case <-rf.healthTicker.C:
			// if it's not leader, break
			if !rf.isLeader() {
				// rf.LogInfo("break")
				break
			}
			if err := rf.handleHealthcheck(true); err != nil {
				rf.LogError("failed on rf.handleHealthcheck: %v", err)
			}
		}
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
	rf.state = STATE_FOLLOWER
	rf.currentTerm = termInit

	/*
		fName := fmt.Sprintf("log-raft-%v.txt", rf.me)
		os.Remove(fName)
		file, err := os.OpenFile(fName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		// multi := zerolog.MultiLevelWriter(os.Stderr, file)

		logger := zerolog.New(multi).With().Timestamp().Logger()
		// Set the global logger to use the configured logger
		log.Logger = logger
	*/

	// zerolog.TimeFieldFormat = zerolog.

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
