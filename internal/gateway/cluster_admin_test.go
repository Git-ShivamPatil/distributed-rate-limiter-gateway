package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// fakeControl is a state machine with no log under it: the entries are applied
// for real, so the handlers are tested against the rules that actually govern
// membership rather than against a stub that always says yes.
type fakeControl struct {
	mu       sync.Mutex
	state    cluster.State
	leader   string
	leaderAt string
	proposed []cluster.Command
}

func (f *fakeControl) Propose(_ context.Context, cmd cluster.Command) (cluster.Applied, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proposed = append(f.proposed, cmd)
	if f.leader != "" {
		return cluster.Applied{}, &cluster.NotLeaderError{LeaderID: f.leader, LeaderAddr: f.leaderAt}
	}
	next, changed, err := f.state.Apply(cmd)
	if err != nil {
		return cluster.Applied{}, err
	}
	f.state = next
	return cluster.Applied{State: next, Changed: changed}, nil
}

func (f *fakeControl) State() cluster.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

func (f *fakeControl) Stats() cluster.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cluster.Stats{NodeID: "gateway-1", State: "Leader", LeaderID: "gateway-1"}
}

func clusterAdminServer(t *testing.T, control ClusterController, token string) *Server {
	t.Helper()
	cfg := testConfig(t)
	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		t.Fatal(err)
	}
	cache := policy.NewCache(policies, policy.WithTTL(time.Hour))
	opts := []Option{WithCache(cache), WithClusterControl(control)}
	if token != "" {
		// Deliberately NO WithAdmin: membership needs a log, not a database,
		// and a gateway with consensus but no policy store must still be able
		// to change its own membership.
		opts = append(opts, WithAdmin(nil, auth.NewAdminToken(token)))
	}
	return New(cfg, decide.New(limiter.NewMemory(), cache, cfg.Node.ID), nil, opts...)
}

func do(t *testing.T, srv *Server, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		r.Header.Set("X-Admin-Token", token)
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, r)
	return w
}

func TestMembershipCanBeChangedWhileTheGatewayRuns(t *testing.T) {
	control := &fakeControl{}
	srv := clusterAdminServer(t, control, "secret")

	w := do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "secret",
		`{"id":"gateway-2","addr":"127.0.0.1:19102"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("adding a member answered %d: %s", w.Code, w.Body)
	}
	var added struct {
		Changed   bool   `json:"changed"`
		RingEpoch uint64 `json:"ring_epoch"`
		Members   int    `json:"members"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &added); err != nil {
		t.Fatal(err)
	}
	if !added.Changed || added.Members != 1 || added.RingEpoch != 1 {
		t.Fatalf("adding a member reported %+v", added)
	}

	// The same request sent twice is the same request. An orchestrator that
	// retries must not be told it failed, and must not walk the epoch up for a
	// change that already happened.
	w = do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "secret",
		`{"id":"gateway-2","addr":"127.0.0.1:19102"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("re-adding a member answered %d: %s", w.Code, w.Body)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &added); err != nil {
		t.Fatal(err)
	}
	if added.Changed || added.RingEpoch != 1 {
		t.Fatalf("re-adding an unchanged member reported %+v", added)
	}

	w = do(t, srv, http.MethodGet, "/admin/v1/cluster/members", "secret", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "gateway-2") {
		t.Fatalf("listing members answered %d: %s", w.Code, w.Body)
	}

	w = do(t, srv, http.MethodDelete, "/admin/v1/cluster/members/gateway-2", "secret", "")
	if w.Code != http.StatusOK {
		t.Fatalf("removing a member answered %d: %s", w.Code, w.Body)
	}
	if control.State().Has("gateway-2") {
		t.Fatal("the member is still in the committed state")
	}

	// Removing something that has already gone is the same request sent twice.
	w = do(t, srv, http.MethodDelete, "/admin/v1/cluster/members/gateway-2", "secret", "")
	if w.Code != http.StatusOK {
		t.Fatalf("removing an absent member answered %d: %s", w.Code, w.Body)
	}
}

// A follower says who to ask rather than guessing, failing silently, or
// writing something only it can see.
func TestAFollowerNamesTheLeaderInsteadOfCommitting(t *testing.T) {
	control := &fakeControl{leader: "gateway-3", leaderAt: "127.0.0.1:19203"}
	srv := clusterAdminServer(t, control, "secret")

	w := do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "secret",
		`{"id":"gateway-2","addr":"127.0.0.1:19102"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("a follower answered %d, want 409: %s", w.Code, w.Body)
	}
	var body struct {
		Error      string `json:"error"`
		Leader     string `json:"leader"`
		LeaderAddr string `json:"leader_addr"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error != "not_leader" || body.Leader != "gateway-3" || body.LeaderAddr == "" {
		t.Fatalf("a follower's refusal does not say who to ask: %+v", body)
	}
	if control.State().Has("gateway-2") {
		t.Fatal("a follower applied a membership change locally")
	}
}

// An entry the state machine refuses is a bad request, not a server failure:
// every node would refuse it identically, so retrying elsewhere is pointless.
func TestARefusedMembershipChangeIsAClientError(t *testing.T) {
	control := &fakeControl{}
	srv := clusterAdminServer(t, control, "secret")

	if w := do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "secret",
		`{"id":"gateway-2","addr":"127.0.0.1:19102"}`); w.Code != http.StatusOK {
		t.Fatalf("setup: %d %s", w.Code, w.Body)
	}
	// Two ids on one address is one process answering as two shards.
	w := do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "secret",
		`{"id":"gateway-9","addr":"127.0.0.1:19102"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("two members on one address answered %d, want 400: %s", w.Code, w.Body)
	}

	// A member with no address: nothing could forward to it.
	w = do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "secret", `{"id":"gateway-9"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("a member with no address answered %d, want 400: %s", w.Code, w.Body)
	}
}

func TestMembershipEndpointsNeedTheAdminToken(t *testing.T) {
	control := &fakeControl{}
	srv := clusterAdminServer(t, control, "secret")

	if w := do(t, srv, http.MethodGet, "/admin/v1/cluster/members", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("an unauthenticated read answered %d, want 401", w.Code)
	}
	if w := do(t, srv, http.MethodPost, "/admin/v1/cluster/members", "wrong",
		`{"id":"gateway-2","addr":"127.0.0.1:19102"}`); w.Code != http.StatusUnauthorized {
		t.Fatalf("a wrong token answered %d, want 401", w.Code)
	}
	if len(control.proposed) != 0 {
		t.Fatalf("%d entries were proposed without credentials", len(control.proposed))
	}
}

// A gateway with no consensus has no membership to change, and must say so
// rather than serving an endpoint that quietly does nothing.
func TestMembershipEndpointsAreAbsentWithoutConsensus(t *testing.T) {
	srv := clusterAdminServer(t, nil, "secret")
	if w := do(t, srv, http.MethodGet, "/admin/v1/cluster/members", "secret", ""); w.Code != http.StatusNotFound {
		t.Fatalf("a gateway with no consensus answered %d, want 404", w.Code)
	}
}

func policyAndClusterServer(t *testing.T, store AdminStore, control ClusterController, token string) *Server {
	t.Helper()
	cfg := testConfig(t)
	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		t.Fatal(err)
	}
	cache := policy.NewCache(policies, policy.WithTTL(time.Hour))
	return New(cfg, decide.New(limiter.NewMemory(), cache, cfg.Node.ID), nil,
		WithCache(cache),
		WithAdmin(store, auth.NewAdminToken(token)),
		WithClusterControl(control))
}

func countProposals(control *fakeControl, kind cluster.CommandKind) int {
	control.mu.Lock()
	defer control.mu.Unlock()
	n := 0
	for _, c := range control.proposed {
		if c.Kind == kind {
			n++
		}
	}
	return n
}

const onePolicy = `{"failure_mode":"closed","limits":[{"name":"per-minute","algorithm":"token_bucket","count":10,"period_ms":60000}]}`

// An edit that changes what limits apply mints a generation, and that is what
// lets the counter store refuse a node still holding the old ones.
func TestAPolicyEditMintsAGeneration(t *testing.T) {
	control := &fakeControl{}
	srv := policyAndClusterServer(t, &fakeAdminStore{}, control, "secret")

	if w := do(t, srv, http.MethodPut, "/admin/v1/policies/free", "secret", onePolicy); w.Code != http.StatusOK {
		t.Fatalf("writing a policy answered %d: %s", w.Code, w.Body)
	}
	if got := countProposals(control, cluster.BumpPolicyGen); got != 1 {
		t.Fatalf("a policy edit minted %d generations, want exactly 1", got)
	}
	if got := control.State().PolicyGen; got != 1 {
		t.Fatalf("the committed generation is %d after one edit", got)
	}

	// Moving a tenant onto a different policy is a limit change too.
	if w := do(t, srv, http.MethodPost, "/admin/v1/tenants", "secret",
		`{"id":"acme","name":"Acme","policy":"free"}`); w.Code != http.StatusOK {
		t.Fatalf("writing a tenant answered %d: %s", w.Code, w.Body)
	}
	if got := countProposals(control, cluster.BumpPolicyGen); got != 2 {
		t.Fatalf("a tenant edit did not mint a generation: %d total", got)
	}
}

// The edit is written before the generation is minted, so a follower has
// already changed the policy store by the time it discovers it cannot mint.
// Reporting that plainly is the difference between an operator re-sending and
// an edit staying un-fenced indefinitely.
func TestAFollowerSaysTheEditLandedButIsNotFenced(t *testing.T) {
	store := &fakeAdminStore{}
	control := &fakeControl{leader: "gateway-2", leaderAt: "127.0.0.1:19202"}
	srv := policyAndClusterServer(t, store, control, "secret")

	w := do(t, srv, http.MethodPut, "/admin/v1/policies/free", "secret", onePolicy)
	if w.Code != http.StatusConflict {
		t.Fatalf("a follower answered %d, want 409: %s", w.Code, w.Body)
	}

	var body struct {
		Error   string `json:"error"`
		Leader  string `json:"leader"`
		Written bool   `json:"written"`
		Fenced  bool   `json:"fenced"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error != "not_leader" || body.Leader != "gateway-2" {
		t.Fatalf("the refusal does not name the leader: %+v", body)
	}
	if !body.Written {
		t.Fatal("the answer claims the edit did not land, but it did -- an operator reading this would not re-send")
	}
	if body.Fenced {
		t.Fatal("the answer claims the edit is fenced, and it is not")
	}
	store.mu.Lock()
	n := len(store.policies)
	store.mu.Unlock()
	if n != 1 {
		t.Fatalf("the policy store holds %d policies; the edit should have landed before the mint was attempted", n)
	}
}

// A gateway with no log writes policies exactly as it always did. Fencing is
// something a cluster does; a single node has nobody to be out of step with.
func TestWithoutALogAPolicyEditStillWorks(t *testing.T) {
	srv := policyAndClusterServer(t, &fakeAdminStore{}, nil, "secret")
	if w := do(t, srv, http.MethodPut, "/admin/v1/policies/free", "secret", onePolicy); w.Code != http.StatusOK {
		t.Fatalf("writing a policy with no consensus answered %d: %s", w.Code, w.Body)
	}
}
