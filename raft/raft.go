package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	Debug           int = 0
	None            int = 0
	HeartbeatCycle      = time.Millisecond * 50
	ElectionMinTime     = 150
	ElectionMaxTime     = 300
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Config struct {
	ID    uint64
	peers []uint64
}

type raft struct {
	mu        sync.Mutex
	peers     []*goraft.ClientEnd
	persister *goraft.Persister
	me        int

	currentTerm int
	voteFor     int
	lead        int
	logs        []LogEntry

	// Volatile state on all servers.
	commitIndex int // index of highest log entry known to be committed.
	lastApplied int // index of highest log entry applied to state machine.

	// Volatile state on leaders.
	nextIndex  []int // index of the next log entry to send to that server.
	matchIndex []int // index of highest log entry applied to state machine.
	// Log and Timer
	// logger *log.Logger
	// granted vote number
	granted_votes_count int

	state string
	// State and applyMsg chan
	applyCh chan ApplyMsg
	timer   *time.Timer
}

func (r *raft) hasLeader() bool { return r.lead != None }

// GetState return currentTerm.
func (r *raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = r.currentTerm
	isleader = (r.state == LEADER)
	return term, isleader
}

func (r *raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(r.currentTerm)
	enc.Encode(r.voteFor)
	enc.Encode(r.logs)
	r.persister.SaveRaftState(buf.Bytes())

}

// readPersist restore previously persisted state.
func (r *raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data != nil {
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		dec.Decode(&r.currentTerm)
		dec.Decode(&r.voteFor)
		dec.Decode(&r.logs)
	}
}

// RequestVoteArgs RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply RPC reply structure.
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (r *raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	may_grant_vote := true
	// current server's log must newer than the candidate.
	if len(r.logs) > 0 {
		if r.logs[len(r.logs)-1].Term > args.LastLogTerm ||
			(r.logs[len(r.logs)-1].Term == args.LastLogIndex &&
				len(r.logs)-1 > args.LastLogIndex) {
			may_grant_vote = false
		}
	}

	// current server's current term bigger than the candidate.
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return
	}

	// current server's current term same as the candidate.
	if args.Term == r.currentTerm {
		// no voted candidate.
		if r.voteFor == -1 && may_grant_vote {
			r.voteFor = args.CandidateId
			r.persist()
		}
		reply.Term = r.currentTerm
		reply.VoteGranted = (r.voteFor == args.CandidateId)
		//if reply.VoteGranted {
		//	r.logger.Printf("Vote for server[%v] at Term:%v\n", r.me, args.CandidateId, r.currentTerm)
		//}
		return
	}

	if args.Term > r.currentTerm {
		r.state = FOLLOWER
		r.currentTerm = args.Term
		r.voteFor = -1

		if may_grant_vote {
			r.voteFor = args.CandidateId
			r.persist()
		}
		r.resetTimer()
		reply.Term = args.Term
		reply.VoteGranted = (r.voteFor == args.CandidateId)
		//if reply.VoteGranted {
		//	r.logger.Printf("Vote for server[%v] at Term:%v\n", rf.me, args.CandidateId, rf.currentTerm)
		//}
		return
	}
}

// majority func
func majority(n int) int {
	return n/2 + 1
}

// handleVoteResult handle vote result.
func (r *raft) handleVoteResult(reply RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// old term ignore
	if reply.Term < r.currentTerm {
		return
	}
	// newer reply item push peer to be follower again
	if reply.Term > r.currentTerm {
		r.currentTerm = reply.Term
		r.state = FOLLOWER
		r.voteFor = -1
		r.resetTimer()
		return
	}

	if r.state == CANDIDATE && reply.VoteGranted {
		r.granted_votes_count += 1
		if r.granted_votes_count >= majority(len(r.peers)) {
			r.state = LEADER
			// r.logger.Printf("Leader at Term:%v log_len:%v\n", rf.me, rf.currentTerm, len(rf.logs))
			for i := 0; i < len(r.peers); i++ {
				if i == r.me {
					continue
				}
				r.nextIndex[i] = len(r.logs)
				r.matchIndex[i] = -1
			}
			r.resetTimer()
		}
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in r.peers[].
// expects RPC arguments in args.
// fills in *reply with PPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared int the
// handler function (including whether they are pointers).
//
// returns true if rpc says the RPC was delivered.
//
// if you're having trouble getting RPC to wore, check that you've
// capitalized all field names in struct passed over RPC, and
// that the caller passes the address of the reply struct with& not
// the struct itself.
func (r *raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("raft.RequestVote", args, reply)
	return ok
}

// AppendEntryArgs RPC arguments structure.
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntryReply PPC reply structure.
type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

// AppendEntries append entries.
func (r *raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		// rf.logger.Printf("Args term:%v less than currentTerm:%v drop it", args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = r.currentTerm
	} else {
		r.state = FOLLOWER
		r.currentTerm = args.Term
		r.voteFor = -1
		reply.Term = args.Term
		// Since at first, leader communicates with followers.
		// nextIndex[server] value equal to len(leader.logs)
		// so system need to find the matching term and index.
		if args.PrevLogIndex >= 0 && (len(r.logs)-1 < args.PrevLogIndex || r.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			// r.logger.Printf("Match failed %v\n", args)
			reply.CommitIndex = len(r.logs) - 1
			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex
			}

			for reply.CommitIndex >= 0 {
				if r.logs[reply.CommitIndex].Term == args.PrevLogTerm {
					break
				}
				reply.CommitIndex--
			}
			reply.Success = false
		} else if args.Entries != nil {

		}
	}

}

// SendAppendEntryToFollower send append entry to a follower.
func (r *raft) SendAppendEntryToFollower(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := r.peers[server].Call("raft.AppendEntries", args, reply)
	return ok
}

// SendAppendEntriesToAllFollower send append entries to all follower.
func (r *raft) SendAppendEntriesToAllFollower() {
	for i := 0; i < len(r.peers); i++ {
		if i == r.me {
			continue
		}
		var args AppendEntryArgs
		args.Term = r.currentTerm
		args.LeaderId = r.me
		args.PrevLogIndex = r.nextIndex[i] - 1
		// r.logger.Printf("prevLogIndx:%v logs_term:%v", args.PrevLogIndex, len(rf.logs))
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = r.logs[args.PrevLogIndex].Term
		}
		if r.nextIndex[i] < len(r.logs) {
			args.Entries = r.logs[r.nextIndex[i]:]
		}
		args.LeaderCommit = r.commitIndex
		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ok := r.SendAppendEntryToFollower(server, args, &reply)
			if ok {
				r.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

// handleAppendEntries handle AppendEntry result.
func (r *raft) handleAppendEntries(server int, reply AppendEntryReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// r.logger.Printf("Got append entries result: %v\n", reply)
	if r.state != LEADER {
		// r.logger.Printf("Lose leader\n")
		return
	}

	// Leader should degenerate to Follower.
	if reply.Term > r.currentTerm {
		r.currentTerm = reply.Term
		r.state = FOLLOWER
		r.voteFor = -1
		r.resetTimer()
		return
	}

	if reply.Success {
		r.nextIndex[server] = reply.CommitIndex + 1
		r.matchIndex[server] = reply.CommitIndex
		reply_count := 1
		for i := 0; i < len(r.peers); i++ {
			if i == r.me {
				continue
			}
			if r.matchIndex[i] >= r.matchIndex[server] {
				reply_count += 1
			}
		}
		if reply_count >= majority(len(r.peers)) &&
			r.commitIndex < r.matchIndex[server] &&
			r.logs[r.matchIndex[server]].Term == r.currentTerm {
			// r.logger.Printf("Update commit index to %v\n", r.matchIndex[server])
			r.commitIndex = r.matchIndex[server]
			go r.commitLogs()
		}
	} else {
		r.nextIndex[server] = reply.CommitIndex + 1
		r.SendAppendEntriesToAllFollower()
	}
}

// commitLogs commit log is send ApplyMsg(a king redo log) to applyCh.
func (r *raft) commitLogs() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.commitIndex > len(r.logs)-1 {
		r.commitIndex = len(r.logs) - 1
	}

	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		// r.logger.Printf("Applying cmd %v\t%v\n", i, r.logs[i].Command)
		r.applyCh <- ApplyMsg{Index: i + 1, Command: r.logs[i].Command}
	}
	r.lastApplied = r.commitIndex
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
func (r *raft) Start(command interface{}) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	nlog := LogEntry{command, r.currentTerm}
	if r.state != LEADER {
		return index, term, isLeader
	}

	isLeader = (r.state == LEADER)
	r.logs = append(r.logs, nlog)
	index = len(r.logs)
	term = r.currentTerm
	r.persist()
	// r.logger.Printf("New command:%v at term:%v\n", index, r.currentTerm)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (r *raft) Kill() {
	// Your code here, if desired.
}

// handleTimer when peer timeout, it changes to be candidate end sendRequestVote.
func (r *raft) handleTimer() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != LEADER {
		r.state = CANDIDATE
		r.currentTerm += 1
		r.voteFor = r.me
		r.granted_votes_count = 1
		r.persist()
		// r.logger.Printf("New election, Candidate:%v term:%v\n", r.me, r.currentTerm)
		args := RequestVoteArgs{
			Term:         r.currentTerm,
			CandidateId:  r.me,
			LastLogIndex: len(r.logs) - 1,
		}

		if len(r.logs) > 0 {
			args.LastLogTerm = r.logs[args.LastLogIndex].Term
		}

		for server := 0; server < len(r.peers); server++ {
			if server == r.me {
				continue
			}

			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := r.sendRequestVote(server, args, &reply)
				if ok {
					r.handleVoteResult(reply)
				}
			}(server, args)
		}
	} else {
		r.SendAppendEntriesToAllFollower()
	}
	r.resetTimer()
}

//
// LEADER(HeartBeat):50ms
// FOLLOWER:150~300
//
func (r *raft) resetTimer() {
	if r.timer == nil {
		r.timer = time.NewTimer(time.Millisecond * 1000)
		go func() {
			for {
				<-r.timer.C
				r.handleTimer()
			}
		}()
	}
	new_timeout := HeartbeatCycle
	if r.state != LEADER {
		new_timeout = time.Microsecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}
	r.timer.Reset(new_timeout)
	// r.logger.Printf("Resetting timeout to %v\n", new_timeout)
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

func Make(peers []*goraft.ClientEnd, me int, persister *goraft.Persister, applyCh chan ApplyMsg) *raft {
	r := &raft{}
	r.peers = peers
	r.persister = persister
	r.me = me

	// Your initialization code here.
	r.currentTerm = 0
	r.voteFor = -1
	r.logs = make([]LogEntry, 0)

	r.commitIndex = -1
	r.lastApplied = -1

	r.nextIndex = make([]int, len(peers))
	r.matchIndex = make([]int, len(peers))

	r.state = FOLLOWER
	r.applyCh = applyCh
	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	/* file, err := os.Create("log" + strconv.Itoa(me) + ".txt")
	if err != nil {
		log.Fatal("failed to create log.txt")
	}
	r.logger = log.New(file, fmt.Sprintf("[Server %v]", me), log.LstdFlags)
	*/
	r.persist()
	r.resetTimer()
	return r
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
