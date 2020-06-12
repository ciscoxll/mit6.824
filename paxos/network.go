package paxos

import "time"

type network interface {
	send(m message)
	recv(timeout time.Duration) (message, bool)
}
