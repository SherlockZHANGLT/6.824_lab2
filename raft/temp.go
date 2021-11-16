package raf

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

//
// log entry contains command for state machine, and term when entry
// was received by leader
//
type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower           = 1
	Candidate          = 2
	Leader             = 3
	HEART_BEAT_TIMEOUT = 100 //心跳超时，要求1秒10次，所以是100ms一次
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimer  *time.Timer // 选举定时器
	heartbeatTimer *time.Timer // 心跳定时器
	state          int         // 角色
	voteCount      int         //投票数

	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

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
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Candidate[raft%v][term:%v] request vote: raft%v[%v] 's term%v\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm)
	reply.VoteGranted = false
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("raft%v don't vote for raft%v\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}
	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId /*&& (rf.lastApplied == args.lastLogIndex && rf.log[rf.lastApplied].term == args.lastLogTerm) */ {
		DPrintf("raft%v vote for raft%v\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(randTimeDuration())
}

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}
type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.currentTerm, rf.me, rf.state)
	reply.Success = true
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}
	rf.electionTimer.Reset(randTimeDuration())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("create raft%v...", me)
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(HEART_BEAT_TIMEOUT * time.Millisecond)
	rf.electionTimer = time.NewTimer(randTimeDuration())

	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				switch rf.state {
				case Follower:
					rf.switchStateTo(Candidate)
				case Candidate:
					rf.startElection()
				}
				rf.mu.Unlock()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.heartbeats()
					rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
				}
				rf.mu.Unlock()
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//发送心跳包
func (rf *Raft) heartbeats() {
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				args := AppendEntriesArgs{}
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.switchStateTo(Follower)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
	time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)
}

func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}

//切换状态，调用者需要加锁
func (rf *Raft) switchStateTo(state int) {
	if state == rf.state {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration())
		rf.votedFor = -1
	case Candidate:
		//成为候选人后立马进行选举
		rf.startElection()

	case Leader:
		rf.electionTimer.Stop()
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	//    DPrintf("raft%v is starting election\n", rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me //vote for me
	rf.electionTimer.Reset(randTimeDuration())
	rf.voteCount = 1

	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				// DPrintf("raft%v[%v] is sending RequestVote RPC to raft%v\n", rf.me, rf.state, peer)
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					if reply.VoteGranted && rf.state == Candidate {
						rf.voteCount += 1
						if rf.voteCount > len(rf.peers)/2 {
							rf.switchStateTo(Leader)
						}
					} else if reply.Term > rf.currentTerm {
						//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
						rf.currentTerm = reply.Term
						rf.switchStateTo(Follower)
					}
					rf.mu.Unlock()
				} else {
					// DPrintf("raft%v[%v] vote:raft%v no reply, currentTerm:%v\n", rf.me, rf.state, peer, rf.currentTerm)
				}
			}(peer)
		}
	}
}
