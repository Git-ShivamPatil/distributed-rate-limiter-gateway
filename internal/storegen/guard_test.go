package storegen

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeStore is a counter store whose identity a test can change the way a
// FLUSHALL, a restart or a replacement would.
type fakeStore struct {
	mu      sync.Mutex
	gen     uint64
	runID   string
	err     error
	mints   int
	readErr error
}

func (s *fakeStore) Identity(context.Context) (uint64, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return 0, "", s.readErr
	}
	return s.gen, s.runID, nil
}

func (s *fakeStore) MintStoreGeneration(_ context.Context, candidate uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mints++
	if s.err != nil {
		return 0, s.err
	}
	if s.gen == 0 {
		s.gen = candidate
	}
	return s.gen, nil
}

// wipe is what a FLUSHALL does: every key gone, and the SAME run id, because
// the server never restarted.
func (s *fakeStore) wipe() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen = 0
}

func (s *fakeStore) restart(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runID = runID
}

type cluster struct {
	mu        sync.Mutex
	committed uint64
	proposals []uint64
	err       error
}

func (c *cluster) get() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.committed
}

func (c *cluster) propose(_ context.Context, gen uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.proposals = append(c.proposals, gen)
	if c.err != nil {
		return c.err
	}
	c.committed = gen // the leader's own state machine applies it
	return nil
}

func newGuard(store *fakeStore, cl *cluster) *Guard {
	return New(Options{Store: store, Committed: cl.get, Propose: cl.propose})
}

// A fresh deployment has agreed on nothing, so there are no counters from an
// older generation to protect. It names the store and carries on; it must not
// refuse, because zero means unfenced everywhere else in this system.
func TestAFreshDeploymentNamesTheStoreWithoutRefusingAnything(t *testing.T) {
	store := &fakeStore{runID: "abc"}
	cl := &cluster{}
	g := newGuard(store, cl)

	if err := g.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); err != nil {
		t.Fatalf("a fresh deployment refused traffic: %v", err)
	}
	if cl.get() != 1 {
		t.Fatalf("the cluster committed generation %d, want 1", cl.get())
	}
	if store.mints != 1 {
		t.Fatalf("the store was minted %d times, want 1", store.mints)
	}
}

// THE CASE THE RUN ID MISSES. A FLUSHALL empties every key and leaves the
// server process -- and therefore its run id -- completely untouched. A
// detector that watched only the run id would see nothing at all, which is why
// the absence of the generation key is a condition in its own right.
func TestAFlushIsCaughtEvenThoughTheRunIdDoesNotChange(t *testing.T) {
	store := &fakeStore{runID: "abc"}
	cl := &cluster{}
	g := newGuard(store, cl)
	ctx := context.Background()

	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	committedBefore := cl.get()

	store.wipe()

	// The control for this test is the run id itself: it has NOT changed, so
	// anything that fires below fired on the missing key alone.
	gen, runID, _ := store.Identity(ctx)
	if runID != "abc" || gen != 0 {
		t.Fatalf("a flush changed the run id (%q) or left a generation (%d); this test would prove nothing", runID, gen)
	}

	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); !errors.Is(err, ErrStoreChanged) {
		t.Fatalf("a wiped store was not detected: Check() = %v", err)
	}

	// And the new generation OUTRANKS the old one, so a node still carrying
	// the old number is fenced rather than waved through.
	if cl.get() <= committedBefore {
		t.Fatalf("the generation after the wipe is %d, which does not outrank the %d before it",
			cl.get(), committedBefore)
	}

	// Once the cluster has agreed on the new generation, it serves again.
	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); err != nil {
		t.Fatalf("still refusing after the cluster agreed on the new generation: %v", err)
	}
	if st := g.Stats(); st.Latches != 1 || st.Denials == 0 {
		t.Fatalf("the latch was not counted: %+v", st)
	}
}

// A store that came back with its data intact is not a store that was
// replaced. Refusing here would turn every Redis restart into an outage.
func TestARestartThatKeptItsDataKeepsServing(t *testing.T) {
	store := &fakeStore{runID: "abc"}
	cl := &cluster{}
	g := newGuard(store, cl)
	ctx := context.Background()

	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	store.restart("def") // new process, same generation, data intact

	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); err != nil {
		t.Fatalf("a restart that kept its data was treated as a replacement: %v", err)
	}
}

// A store whose generation is simply DIFFERENT -- someone pointed the gateway
// at another Redis -- is caught by the comparison, in either direction.
func TestADifferentStoreIsRefused(t *testing.T) {
	store := &fakeStore{gen: 9, runID: "abc"}
	cl := &cluster{committed: 4}
	g := newGuard(store, cl)

	if err := g.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); !errors.Is(err, ErrStoreChanged) {
		t.Fatalf("a store on a different generation was accepted: %v", err)
	}
	if store.mints != 0 {
		t.Fatal("a store that already had a generation was minted again")
	}
}

// A store we cannot READ is an outage, not a reset. The tenant's failure mode
// already decides what an unreachable store means, and latching here would
// turn every network blip into a cluster-wide refusal that outlives it.
func TestAnUnreadableStoreIsNotTreatedAsAReset(t *testing.T) {
	store := &fakeStore{gen: 3, runID: "abc"}
	cl := &cluster{committed: 3}
	g := newGuard(store, cl)
	ctx := context.Background()

	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	store.mu.Lock()
	store.readErr = errors.New("dial tcp: connection refused")
	store.mu.Unlock()

	if err := g.Reconcile(ctx); err == nil {
		t.Fatal("an unreadable store was reported as reconciled")
	}
	if err := g.Check(); err != nil {
		t.Fatalf("an unreachable store latched the guard closed: %v", err)
	}
}

// A follower cannot commit, and that is not a fault: it stays closed until the
// leader does it and its own state machine catches up.
func TestAFollowerStaysClosedUntilTheLeaderAgrees(t *testing.T) {
	store := &fakeStore{gen: 7, runID: "abc"}
	cl := &cluster{committed: 3, err: errors.New("not the leader")}
	g := newGuard(store, cl)
	ctx := context.Background()

	if err := g.Reconcile(ctx); err != nil {
		t.Fatalf("a follower reported an error for something that is not its job: %v", err)
	}
	if err := g.Check(); !errors.Is(err, ErrStoreChanged) {
		t.Fatalf("a follower that could not commit kept serving: %v", err)
	}

	// The leader commits it; this node's state machine applies it.
	cl.mu.Lock()
	cl.committed, cl.err = 7, nil
	cl.mu.Unlock()

	if err := g.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); err != nil {
		t.Fatalf("still refusing after the cluster agreed: %v", err)
	}
}

// The in-band path latches the SAME guard. One store, one answer: a node that
// refused one tenant because the store was replaced has no business serving
// the next one as though it had not been.
func TestTheInBandFenceClosesTheWholeNode(t *testing.T) {
	store := &fakeStore{gen: 3, runID: "abc"}
	cl := &cluster{committed: 3}
	g := newGuard(store, cl)

	if err := g.Check(); err != nil {
		t.Fatalf("closed before anything happened: %v", err)
	}
	g.Fence("a check for acme was refused by the store's own generation")
	if err := g.Check(); !errors.Is(err, ErrStoreChanged) {
		t.Fatalf("an in-band fence did not close the node: %v", err)
	}
	if st := g.Stats(); !st.Blocked || st.Latches != 1 {
		t.Fatalf("stats do not report the latch: %+v", st)
	}
}
