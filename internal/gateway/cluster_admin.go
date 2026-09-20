package gateway

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

// ClusterController is the write side of the replicated membership.
//
// It is an interface rather than *cluster.Consensus so that this package can
// be tested without standing up a consensus cluster, and so that a gateway
// built without one simply has no membership endpoints rather than endpoints
// that answer wrongly.
type ClusterController interface {
	Propose(ctx context.Context, cmd cluster.Command) (cluster.Applied, error)
	State() cluster.State
	Stats() cluster.Stats
}

// memberDTO is one shard as the admin API takes and returns it.
type memberDTO struct {
	ID string `json:"id"`
	// Addr is the gRPC address forwards go to. It is the only address the
	// ring needs, which is why it is the only one the committed state holds.
	Addr string `json:"addr"`
}

// mountClusterAdmin adds the membership endpoints, behind the admin token.
//
// Membership is the one thing in this system that a person changes while it is
// running, and it is the one thing consensus exists for. Everything else on
// the admin API edits rows in Postgres; these two endpoints append to the log.
func (s *Server) mountClusterAdmin(r chi.Router) {
	if s.control == nil {
		return
	}
	r.Get("/members", s.adminListMembers)
	r.Post("/members", s.adminAddMember)
	r.Delete("/members/{id}", s.adminRemoveMember)
}

func (s *Server) adminListMembers(w http.ResponseWriter, _ *http.Request) {
	state := s.control.State()
	members := make([]memberDTO, 0, len(state.Members))
	for _, m := range state.Members {
		members = append(members, memberDTO{ID: m.ID, Addr: m.Addr})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"members":    members,
		"ring_epoch": state.RingEpoch,
		"vnodes":     state.VNodes,
		"raft":       s.control.Stats(),
	})
}

func (s *Server) adminAddMember(w http.ResponseWriter, r *http.Request) {
	var in memberDTO
	if err := decodeJSON(w, r, &in); err != nil {
		return
	}
	if in.ID == "" || in.Addr == "" {
		writeError(w, http.StatusBadRequest, "invalid",
			"a member needs an id and an address; without an address nothing could forward to it")
		return
	}
	s.propose(w, r, cluster.Command{Kind: cluster.AddShard, Member: ring.Node{ID: in.ID, Addr: in.Addr}})
}

func (s *Server) adminRemoveMember(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "invalid", "no member named")
		return
	}
	s.propose(w, r, cluster.Command{Kind: cluster.RemoveShard, ID: id})
}

// bumpPolicyGen mints the next policy generation, after an edit has landed.
//
// The ORDER is Postgres first, then the log, and the failure it leaves behind
// is the survivable one. A crash between the two means the edit is live but
// un-fenced: a node still holding the old limits is not refused, which is
// exactly the behaviour this gateway had before fences existed. The other
// order would mint a generation for an edit that never happened, fencing every
// node until each refreshed and found nothing had changed.
//
// It reports whether the caller should stop, having already answered.
func (s *Server) bumpPolicyGen(w http.ResponseWriter, r *http.Request, what string) bool {
	if s.control == nil {
		return false // no log; policies are unfenced, which is the single-node case
	}

	_, err := s.control.Propose(r.Context(), cluster.Command{Kind: cluster.BumpPolicyGen})
	if err == nil {
		return false
	}

	var notLeader *cluster.NotLeaderError
	if errors.As(err, &notLeader) {
		// The edit is already written. Saying so matters: an operator who
		// reads "not leader" as "nothing happened" will not re-send, and the
		// edit stays un-fenced indefinitely. Re-sending the same request to
		// the leader finishes the job, because both halves are idempotent.
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":       "not_leader",
			"message":     what + " was written, but the generation that fences it is minted by the leader; re-send this request there",
			"leader":      notLeader.LeaderID,
			"leader_addr": notLeader.LeaderAddr,
			"written":     true,
			"fenced":      false,
		})
		return true
	}

	s.log.Error("the edit landed but its generation did not; nodes holding the old policy will not be refused",
		"what", what, "err", err)
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":   "generation_not_minted",
		"message": what + " was written, but the generation that fences it could not be committed",
		"written": true,
		"fenced":  false,
	})
	return true
}

// propose commits one entry and reports what it did.
//
// A node that is not the leader answers 409 and NAMES the leader rather than
// redirecting to it. The committed state carries a member's gRPC address,
// because that is the only address the ring needs; it does not carry an HTTP
// address, and adding one so that this handler could send a redirect would put
// a field in the replicated state for the sole benefit of an error path. The
// caller is told who to ask, which is the same contract every other
// consensus-backed service offers.
func (s *Server) propose(w http.ResponseWriter, r *http.Request, cmd cluster.Command) {
	res, err := s.control.Propose(r.Context(), cmd)

	var notLeader *cluster.NotLeaderError
	if errors.As(err, &notLeader) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":       "not_leader",
			"message":     "membership is changed through the leader; ask it instead",
			"leader":      notLeader.LeaderID,
			"leader_addr": notLeader.LeaderAddr,
		})
		return
	}
	if errors.Is(err, cluster.ErrNotApplicable) {
		// The entry was understood and refused, and every node would refuse
		// it identically. That is a bad request, not a server failure.
		writeError(w, http.StatusBadRequest, "not_applicable", err.Error())
		return
	}
	if err != nil {
		s.log.Error("could not commit a membership change", "kind", cmd.Kind, "err", err)
		writeError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		// changed=false is a success: re-adding a member that is already
		// there, or removing one that has already gone, is the same request
		// sent twice and must not be an error.
		"changed":    res.Changed,
		"ring_epoch": res.State.RingEpoch,
		"members":    len(res.State.Members),
	})
}
