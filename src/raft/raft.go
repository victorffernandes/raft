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
	"_raft/src/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

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

type ApplyMsgReply struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	isLeader   bool
	actualTerm int
	votedFor   int

	electionTimeout *time.Ticker
	leaderTimeout   *time.Ticker

	applyMsgChannel       chan ApplyMsg
	shouldBecomeCandidate bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	return rf.actualTerm, rf.isLeader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Println("REQUEST VOTE")
	// fmt.Println(args, reply)
	if args.Term > rf.actualTerm {
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.actualTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.isLeader = false
	} else if args.Term == rf.actualTerm {
		if args.CandidateId == rf.votedFor {
			reply.VoteGranted = true
			reply.Term = rf.actualTerm
			rf.isLeader = false
		} else {
			reply.VoteGranted = false
			reply.Term = rf.actualTerm
		}
	} else {
		reply.VoteGranted = false
		reply.Term = rf.actualTerm
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *ApplyMsg, reply *ApplyMsgReply) {
	// fmt.Println("REQUEST VOTE")
	// fmt.Println(args, reply)
	// fmt.Println("Calling append entries: node ", rf.me)
	rf.applyMsgChannel <- *args
	return
	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, reply *RequestVoteReply) bool {
	var args *RequestVoteArgs = new(RequestVoteArgs)

	args.Term = rf.actualTerm + 1
	args.CandidateId = rf.me

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *ApplyMsg, reply *ApplyMsgReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) initiateElectionTerm() {
	// fmt.Println("Election started for node: ", rf.me)
	var term = rf.actualTerm + 1
	totalVotes := 0.0
	total := 0.0

	// fmt.Println("Initiated election term", term)
	for index := range rf.peers {
		if index != rf.me {
			total += 1
			var reply *RequestVoteReply = new(RequestVoteReply)
			receivedResponse := rf.sendRequestVote(index, reply)

			if receivedResponse && reply.VoteGranted {
				totalVotes += 1
			}
			if receivedResponse && !reply.VoteGranted && reply.Term > term {
				rf.actualTerm = reply.Term
				return
			}
		}
	}

	fmt.Println("Election total", totalVotes, ":", total)
	if totalVotes/total >= 0.5 {
		rf.isLeader = true
		rf.actualTerm = term

		var duration time.Duration = time.Duration((100) * 1000 * 1000)
		rf.leaderTimeout = time.NewTicker(duration)
		// fmt.Println("Leader elected. Node:  ", rf.me, " time out: ", duration)

		rf.sendApplyMessage(0, 0)
		rf.electionTimeout.Stop()
		go func() {
			for {
				<-rf.leaderTimeout.C
				rf.sendApplyMessage(0, 0)
			}
		}()
	}
}

func (rf *Raft) sendApplyMessage(i int, command interface{}) {
	var args *ApplyMsg = new(ApplyMsg)
	args.Index = i
	args.Command = command
	var reply *ApplyMsgReply = new(ApplyMsgReply)
	for index := range rf.peers {
		rf.sendAppendEntries(index, args, reply)
	}

	// fmt.Println("Sending applyMsg")
	// rf.applyMsgChannel <- sampleMsg
	// // rf.sendSampleMsgChannel()
	// var sampleMsg ApplyMsg
	// // fmt.Println("Sending applyMsg")
	// rf.applyMsgChannel <- sampleMsg
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
	if !rf.isLeader {
		return 0, rf.actualTerm, rf.isLeader
	}

	index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).

	return index, rf.actualTerm, rf.isLeader
}

func GenerateRandomInterval() int {
	return (150 + rand.Intn(150)) * 1000 * 1000
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

	rf.isLeader = false
	rf.actualTerm = 1

	// Your initialization code here (2A, 2B, 2C).
	var duration time.Duration = time.Duration(GenerateRandomInterval())
	rf.electionTimeout = time.NewTicker(duration)
	fmt.Println("Initial timeout election", duration, " for node: ", rf.me)

	rf.applyMsgChannel = applyCh

	go func() {
		for {
			<-rf.applyMsgChannel
			var duration time.Duration = time.Duration(GenerateRandomInterval())
			// fmt.Println("Node:", rf.me, " isLeader: ", rf.isLeader, " term: ", rf.actualTerm)
			rf.electionTimeout.Reset(duration)
			// fmt.Println("Apply msg timeout")
		}
	}()

	go func() {
		for {
			<-rf.electionTimeout.C
			rf.initiateElectionTerm()
			//fmt.Println("Election timeout")
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Println("im here")
	return rf
}
