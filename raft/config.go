package raft

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/ciscoxll/mit6.824/labrpc"
	"log"
	"runtime"
	"sync"
	"testing"
)

type config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *labrpc.Network
	n         int
	done      int32 // tell internal threads to die.
	rafts     []*raft
	applyErr  []string // form apply channel readers.
	connected []bool   // whether each server is on the net.
	saved     []*Persister
	endNames  [][]string    // the port file names each sends to.
	logs      []map[int]int // copy of each server's committed entries.
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func make_config(t *testing.T, n int, unreliable bool) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endNames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]int, cfg.n)

	cfg.setUnreliable(unreliable)
	cfg.net.LongDelays(true)

	// create a full set of rafts.
	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]int{}
		cfg.start1(i)
	}

	// connect everyone.
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}
	return cfg
}

// shut down a raft server but save its persist state.
func (cfg *config) crash1(i int) {}

// start or re-start a raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// start persist, to isolate previous instance of
// this server, since we cannot really kill it.
func (cfg *config) start1(i int) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endNames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; i++ {
		cfg.endNames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endNames[i][j])
		cfg.net.Connect(cfg.endNames[i][j], j)
	}

	cfg.mu.Lock()

	// a fresh persist, do olf instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persist content so that we always
	// pass make() the last persis state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	// listen to messages from raft indicating newly committed message.
	applyCh := make(chan ApplyMsg)
	go func() {
		for m := range applyCh {
			err_msg := ""
			if m.UseSnapshot {
				// ignore the snapshot.
			} else if v, ok := (m.Command).(int); ok {
				cfg.mu.Lock()
				for j := 0; j < len(cfg.logs); j++ {
					if old, oldOk := cfg.logs[j][m.Index]; oldOk && old != v {
						// some server has already committed a different value for this entry.
						err_msg = fmt.Sprintf("commit index = %v server = %v %v != server = %v %v",
							m.Index, i, m.Command, j, old)
					}
				}
				_, prevOK := cfg.logs[i][m.Index-1]
				cfg.logs[i][m.Index] = v
				cfg.mu.Unlock()

				if m.Index > 1 && prevOK == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.Index)
				} else {
					err_msg = fmt.Sprintf("committed command %v is not an int", m.Command)
				}

				if err_msg != "" {
					log.Fatalf("apply error : %v", err_msg)
					cfg.applyErr[i] = err_msg
					// keep reading after error so that Raft doesn't block
					// holding locks...
				}
			}
		}
	}()

	rf := Make(ends, i, cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) setUnreliable(unRel bool) {
	cfg.net.Reliable(!unRel)
}
