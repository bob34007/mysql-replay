package stats

import (
	"sync"
	"sync/atomic"
)

const (
	Packets      = "packets"
	Queries      = "queries"
	Streams      = "streams"
	Connections  = "connections"
	ConnWaiting  = "conn.waiting"
	ConnRunning  = "conn.running"
	StmtExecutes = "stmt.executes"
	StmtPrepares = "stmt.prepares"

	FailedQueries      = "err.queries"
	FailedStmtExecutes = "err.stmt.executes"
	FailedStmtPrepares = "err.stmt.prepares"
)

var (
	nPackets      int64
	nQueries      int64
	nStreams      int64
	nConns        int64
	nStmtExecutes int64
	nStmtPrepares int64

	nErrQueries      int64
	nErrStmtExecutes int64
	nErrStmtPrepares int64

	nRunningConns int64
	nWaitingConns int64

	metrics = []string{Packets, Queries, StmtExecutes, StmtPrepares, Streams, Connections, FailedQueries, FailedStmtExecutes, FailedStmtPrepares, ConnWaiting, ConnRunning}
	others  = make(map[string]int64)
	lock    sync.RWMutex
)

func Add(name string, delta int64) int64 {
	switch name {
	case Packets:
		return atomic.AddInt64(&nPackets, delta)
	case ConnRunning:
		return atomic.AddInt64(&nRunningConns, delta)
	case ConnWaiting:
		return atomic.AddInt64(&nWaitingConns, delta)
	case Queries:
		return atomic.AddInt64(&nQueries, delta)
	case StmtExecutes:
		return atomic.AddInt64(&nStmtExecutes, delta)
	case StmtPrepares:
		return atomic.AddInt64(&nStmtPrepares, delta)
	case Streams:
		return atomic.AddInt64(&nStreams, delta)
	case Connections:
		return atomic.AddInt64(&nConns, delta)
	case FailedQueries:
		return atomic.AddInt64(&nErrQueries, delta)
	case FailedStmtExecutes:
		return atomic.AddInt64(&nErrStmtExecutes, delta)
	case FailedStmtPrepares:
		return atomic.AddInt64(&nErrStmtPrepares, delta)
	default:
		lock.Lock()
		defer lock.Unlock()
		others[name] += delta
		return others[name]
	}
}

func Get(name string) int64 {
	switch name {
	case Packets:
		return atomic.LoadInt64(&nPackets)
	case ConnRunning:
		return atomic.LoadInt64(&nRunningConns)
	case ConnWaiting:
		return atomic.LoadInt64(&nWaitingConns)
	case Queries:
		return atomic.LoadInt64(&nQueries)
	case StmtExecutes:
		return atomic.LoadInt64(&nStmtExecutes)
	case StmtPrepares:
		return atomic.LoadInt64(&nStmtPrepares)
	case Streams:
		return atomic.LoadInt64(&nStreams)
	case Connections:
		return atomic.LoadInt64(&nConns)
	case FailedQueries:
		return atomic.LoadInt64(&nErrQueries)
	case FailedStmtExecutes:
		return atomic.LoadInt64(&nErrStmtExecutes)
	case FailedStmtPrepares:
		return atomic.LoadInt64(&nErrStmtPrepares)
	default:
		lock.RLock()
		defer lock.RUnlock()
		return others[name]
	}
}

func Dump() map[string]int64 {
	out := make(map[string]int64, len(metrics)+len(others))
	for _, name := range metrics {
		out[name] = Get(name)
	}
	lock.RLock()
	for k, v := range others {
		out[k] = v
	}
	lock.RUnlock()
	return out
}
