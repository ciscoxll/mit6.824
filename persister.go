package goraft

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func (ps *Persister) SaveRaftState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = data
}
