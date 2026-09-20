package cluster

import (
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM is the replicated state machine: it applies committed entries to a
// State and hands the result to whoever is serving requests.
//
// Everything it can do is in state.go, which is a pure function of (state,
// entry). This file is only the plumbing that connects that function to the
// log, and it is deliberately thin -- a state machine with logic in its
// transport adapter is a state machine whose behaviour depends on which
// transport delivered the entry.
type FSM struct {
	mu    sync.RWMutex
	state State

	// onApply is called, holding no lock, after every entry that changed the
	// state. It is how a committed membership reaches the ring the request
	// path reads. It must not block: it runs on Raft's apply goroutine, and
	// stalling there stalls replication.
	onApply func(State)
}

// NewFSM builds the state machine. onApply may be nil.
func NewFSM(onApply func(State)) *FSM { return &FSM{onApply: onApply} }

// Applied is what Apply returns through the future, so a caller that proposed
// an entry learns whether it changed anything and why not.
type Applied struct {
	State   State
	Changed bool
	Err     error
}

// Apply runs one committed entry.
//
// An entry this build cannot decode or will not apply is reported back to the
// proposer and SKIPPED, not panicked over -- but only because the verdict is
// deterministic: every node decodes the same bytes against the same rules and
// reaches the same answer, so a refusal keeps the replicas identical rather
// than splitting them.
func (f *FSM) Apply(l *raft.Log) any {
	cmd, err := DecodeCommand(l.Data)
	if err != nil {
		return Applied{Err: err}
	}

	f.mu.Lock()
	next, changed, err := f.state.Apply(cmd)
	if err == nil && changed {
		f.state = next
	}
	f.mu.Unlock()

	if err != nil {
		return Applied{Err: err}
	}
	if changed {
		f.notify(next)
	}
	return Applied{State: next, Changed: changed}
}

func (f *FSM) notify(s State) {
	if f.onApply != nil {
		f.onApply(s)
	}
}

// State returns the last applied state.
func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

// Snapshot captures the state so the log before it can be discarded.
//
// The state is a few hundred bytes -- a member list, a vnode count and three
// integers -- so it is copied under the lock and written outside it. A
// snapshot that streamed from live state instead would have to hold the lock
// across a disk write, which would stall every apply behind it.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := f.state.Encode()
	if err != nil {
		return nil, err
	}
	return stateSnapshot(b), nil
}

// Restore replaces the state wholesale from a snapshot.
//
// It replaces rather than merges. A node restoring a snapshot is being told
// what the cluster agreed, and anything it had applied on its own before that
// point is either already in the snapshot or was never committed.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("cluster: reading snapshot: %w", err)
	}
	s, err := DecodeState(b)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.state = s
	f.mu.Unlock()

	f.notify(s)
	return nil
}

type stateSnapshot []byte

func (s stateSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("cluster: writing snapshot: %w", err)
	}
	return sink.Close()
}

func (s stateSnapshot) Release() {}

var (
	_ raft.FSM         = (*FSM)(nil)
	_ raft.FSMSnapshot = stateSnapshot(nil)
)
