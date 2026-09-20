package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

// Peer is one shard, at both of the addresses it answers on.
//
// The two are separate because they carry different traffic and fail
// differently: Addr is where a forwarded check goes, and RaftAddr is where
// replication goes. Running consensus on the request path's port would put a
// leader election and a tenant's traffic in one queue.
type Peer struct {
	ID       string
	Addr     string
	RaftAddr string
}

// ConsensusOptions configures this node's membership of the log.
type ConsensusOptions struct {
	// NodeID must be stable across restarts: it is what the log records as
	// having voted, and a node that comes back under a new id is a new node.
	NodeID string
	// Peers is the bootstrap member set. After the first entry commits, the
	// log is the truth and this is not consulted again.
	Peers []Peer
	// VNodes seeds the ring configuration.
	VNodes int
	// Dir holds the log and the snapshots. Empty keeps both in memory, which
	// is for tests and for a single node that has nothing to recover.
	Dir string
	// Advertise overrides the address peers are told to reach this node on,
	// for the case where the bind address is not routable (a container
	// binding 0.0.0.0).
	Advertise string

	// Timeouts. Zero takes hashicorp/raft's defaults, which are tuned for a
	// datacenter; tests shorten them.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration

	// Transport replaces the TCP transport. Tests pass an in-memory one so a
	// three-node cluster needs no sockets.
	Transport raft.Transport

	// OnApply receives every committed change, and is how a new membership
	// reaches the ring the request path reads. It is set here rather than
	// afterwards because raft starts applying as soon as the node is built,
	// and a hook installed a moment later would miss the entries that arrived
	// in between.
	OnApply func(State)

	Logger *slog.Logger
	// RaftLogger is where hashicorp/raft's own logging goes. Nil sends
	// warnings and errors to stderr and discards the rest: raft at info level
	// narrates every heartbeat, which buries the gateway's own log.
	RaftLogger hclog.Logger
}

// Consensus is this node's membership of the replicated log.
//
// It governs membership, ring configuration and three generations -- and
// nothing else. No counter and no admission decision passes through here.
// That is not a simplification: one to three fsync-bounded round trips per
// check is not a latency regression, it is an impossibility, and the whole
// design exists so that correctness never needed consensus in the first place.
type Consensus struct {
	raft   *raft.Raft
	fsm    *FSM
	self   string
	peers  []Peer
	vnodes int
	log    *slog.Logger

	closers []func() error
}

// NotLeaderError says this node cannot commit, and who can.
//
// It carries the leader rather than just refusing, because every caller that
// gets it has the same next move: ask that node instead.
type NotLeaderError struct {
	LeaderID   string
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "cluster: this node is not the leader, and no leader is known"
	}
	return fmt.Sprintf("cluster: this node is not the leader; %s is", e.LeaderID)
}

// ErrNoLeader means there is no leader to ask yet.
var ErrNoLeader = errors.New("cluster: no leader")

// StartConsensus brings this node into the log.
//
// Bootstrapping is derived, not configured. The node whose id sorts first in
// the member list creates the cluster, with every configured peer already in
// the voter set; the rest start with an empty log and learn the configuration
// from the leader. Every node computes the same answer from the same list, so
// there is no flag an operator can set on two nodes and split the cluster in
// half -- and a node that already has a log never bootstraps again, whatever
// the file says.
func StartConsensus(opts ConsensusOptions) (*Consensus, error) {
	if opts.NodeID == "" {
		return nil, errors.New("cluster: consensus needs a node id")
	}
	if len(opts.Peers) == 0 {
		return nil, errors.New("cluster: consensus needs at least one peer")
	}
	self, ok := findPeer(opts.Peers, opts.NodeID)
	if !ok {
		return nil, fmt.Errorf("cluster: node %q is not in its own peer list", opts.NodeID)
	}

	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	rlog := opts.RaftLogger
	if rlog == nil {
		rlog = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: os.Stderr})
	}

	c := &Consensus{self: opts.NodeID, peers: opts.Peers, vnodes: opts.VNodes, log: log}
	c.fsm = NewFSM(opts.OnApply)

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(opts.NodeID)
	conf.Logger = rlog
	if opts.HeartbeatTimeout > 0 {
		conf.HeartbeatTimeout = opts.HeartbeatTimeout
	}
	if opts.ElectionTimeout > 0 {
		conf.ElectionTimeout = opts.ElectionTimeout
	}
	if opts.LeaderLeaseTimeout > 0 {
		conf.LeaderLeaseTimeout = opts.LeaderLeaseTimeout
	}
	if opts.CommitTimeout > 0 {
		conf.CommitTimeout = opts.CommitTimeout
	}

	logs, stable, snaps, err := c.openStores(opts, rlog)
	if err != nil {
		return nil, err
	}

	transport := opts.Transport
	if transport == nil {
		transport, err = tcpTransport(self, opts.Advertise, rlog)
		if err != nil {
			c.closeAll()
			return nil, err
		}
	}
	if closer, ok := transport.(io.Closer); ok {
		c.closers = append(c.closers, closer.Close)
	}

	existing, err := raft.HasExistingState(logs, stable, snaps)
	if err != nil {
		c.closeAll()
		return nil, fmt.Errorf("cluster: reading the existing log: %w", err)
	}
	if !existing && bootstrapper(opts.Peers) == opts.NodeID {
		servers := make([]raft.Server, 0, len(opts.Peers))
		for _, p := range opts.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(raftAddress(p)),
			})
		}
		if err := raft.BootstrapCluster(conf, logs, stable, snaps, transport, raft.Configuration{Servers: servers}); err != nil {
			c.closeAll()
			return nil, fmt.Errorf("cluster: bootstrapping: %w", err)
		}
		log.Info("bootstrapped the cluster", "node", opts.NodeID, "voters", len(servers))
	}

	r, err := raft.NewRaft(conf, c.fsm, logs, stable, snaps, transport)
	if err != nil {
		c.closeAll()
		return nil, fmt.Errorf("cluster: %w", err)
	}
	c.raft = r
	return c, nil
}

func (c *Consensus) openStores(opts ConsensusOptions, rlog hclog.Logger) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	if opts.Dir == "" {
		return raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), nil
	}
	if err := os.MkdirAll(opts.Dir, 0o750); err != nil {
		return nil, nil, nil, fmt.Errorf("cluster: %w", err)
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(opts.Dir, "raft.db"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cluster: opening the log: %w", err)
	}
	c.closers = append(c.closers, store.Close)

	// Three retained snapshots rather than one: a snapshot that turns out to
	// be corrupt is only discovered when it is restored, and by then the log
	// it replaced is gone.
	snaps, err := raft.NewFileSnapshotStoreWithLogger(opts.Dir, 3, rlog)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cluster: opening the snapshot store: %w", err)
	}
	return store, store, snaps, nil
}

func tcpTransport(self Peer, advertise string, rlog hclog.Logger) (raft.Transport, error) {
	bind := raftAddress(self)
	if advertise == "" {
		advertise = bind
	}
	addr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return nil, fmt.Errorf("cluster: resolving the advertised address %q: %w", advertise, err)
	}
	t, err := raft.NewTCPTransportWithLogger(bind, addr, 3, 10*time.Second, rlog)
	if err != nil {
		return nil, fmt.Errorf("cluster: listening on %s: %w", bind, err)
	}
	return t, nil
}

// Seed commits the configured membership, but only into an empty state.
//
// After the first membership entry commits, the log is the truth and the
// configuration file is never consulted again -- otherwise a node removed
// from the ring through the admin API would be put back by the next leader
// that read a stale file.
func (c *Consensus) Seed(ctx context.Context) error {
	if len(c.fsm.State().Members) > 0 {
		return nil
	}
	members := make([]ring.Node, 0, len(c.peers))
	for _, p := range c.peers {
		members = append(members, ring.Node{ID: p.ID, Addr: p.Addr})
	}
	// One entry, so that no node ever routes on a half-delivered membership.
	if _, err := c.Propose(ctx, Command{Kind: SeedRing, Members: members, VNodes: c.vnodes}); err != nil {
		return err
	}
	c.log.Info("seeded the ring from the configuration", "members", len(c.peers), "vnodes", c.vnodes)
	return nil
}

// Propose commits one entry. Only the leader can; anybody else gets a
// NotLeaderError naming who to ask.
func (c *Consensus) Propose(ctx context.Context, cmd Command) (Applied, error) {
	if c.raft.State() != raft.Leader {
		return Applied{}, c.notLeader()
	}
	b, err := cmd.Encode()
	if err != nil {
		return Applied{}, err
	}

	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 {
			timeout = d
		}
	}

	f := c.raft.Apply(b, timeout)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return Applied{}, c.notLeader()
		}
		return Applied{}, fmt.Errorf("cluster: committing %s: %w", cmd.Kind, err)
	}
	res, ok := f.Response().(Applied)
	if !ok {
		return Applied{}, fmt.Errorf("cluster: committing %s: the state machine answered %T", cmd.Kind, f.Response())
	}
	if res.Err != nil {
		return res, res.Err
	}
	return res, nil
}

func (c *Consensus) notLeader() error {
	id, addr := c.Leader()
	return &NotLeaderError{LeaderID: id, LeaderAddr: addr}
}

// Leader reports the current leader's node id and its consensus address, or
// empty strings when there is no leader.
func (c *Consensus) Leader() (id string, addr string) {
	a, sid := c.raft.LeaderWithID()
	return string(sid), string(a)
}

// IsLeader reports whether this node can commit.
func (c *Consensus) IsLeader() bool { return c.raft.State() == raft.Leader }

// LeaderCh reports leadership changes for this node. It is buffered by
// hashicorp/raft and drops rather than blocks, so a reader that misses a
// transition sees the next one; it is a trigger, never a source of truth.
func (c *Consensus) LeaderCh() <-chan bool { return c.raft.LeaderCh() }

// State returns the last applied state.
func (c *Consensus) State() State { return c.fsm.State() }

// WaitForLeader blocks until some node is leading, or the context ends.
//
// It waits for a condition rather than a duration on purpose: how long an
// election takes is a property of raft's timers and of the machine, and
// anything that asserts on it is asserting on the machine.
func (c *Consensus) WaitForLeader(ctx context.Context) error {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		if id, _ := c.Leader(); id != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrNoLeader, ctx.Err())
		case <-t.C:
		}
	}
}

// Stats is what the cluster endpoint reports about consensus.
type Stats struct {
	NodeID     string `json:"node_id"`
	State      string `json:"state"`
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`
	LastIndex  uint64 `json:"last_index"`
	AppliedAt  uint64 `json:"applied_index"`
}

// Stats reports this node's consensus position.
func (c *Consensus) Stats() Stats {
	id, addr := c.Leader()
	return Stats{
		NodeID:     c.self,
		State:      c.raft.State().String(),
		LeaderID:   id,
		LeaderAddr: addr,
		LastIndex:  c.raft.LastIndex(),
		AppliedAt:  c.raft.AppliedIndex(),
	}
}

// Close stops this node, transferring leadership first when it can.
//
// Transferring rather than simply stopping is what turns a rolling restart
// into a handover: a leader that exits without one leaves the cluster to
// notice by timeout, and every entry proposed in between fails.
func (c *Consensus) Close() error {
	if c.raft != nil {
		if c.raft.State() == raft.Leader && len(c.peers) > 1 {
			if err := c.raft.LeadershipTransfer().Error(); err != nil {
				c.log.Debug("leadership transfer on shutdown did not complete", "err", err)
			}
		}
		if err := c.raft.Shutdown().Error(); err != nil {
			return fmt.Errorf("cluster: shutting down: %w", err)
		}
	}
	return c.closeAll()
}

func (c *Consensus) closeAll() error {
	var err error
	for i := len(c.closers) - 1; i >= 0; i-- {
		if e := c.closers[i](); e != nil && err == nil {
			err = e
		}
	}
	c.closers = nil
	return err
}

func findPeer(peers []Peer, id string) (Peer, bool) {
	for _, p := range peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

// bootstrapper is the id that sorts first. Derived rather than configured, so
// two nodes cannot both believe it is them.
func bootstrapper(peers []Peer) string {
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids[0]
}

// raftAddress is where a peer's consensus traffic goes. A member with no
// consensus address of its own is reached on its request address, which is
// what a single-node deployment ends up with.
func raftAddress(p Peer) string {
	if p.RaftAddr != "" {
		return p.RaftAddr
	}
	return p.Addr
}
