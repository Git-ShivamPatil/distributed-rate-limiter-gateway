package config

import (
	"strings"
	"testing"
	"time"
)

// A three-node cluster with consensus on, written the way cluster-up.sh writes
// it. If this stops parsing, the script that generates it has stopped working.
const raftYAML = `
cluster:
  vnodes: 256
  raft:
    enabled: true
    dir: /tmp/raft-gateway-1
    heartbeat_timeout: 500ms
    election_timeout: 500ms
    leader_lease_timeout: 250ms
    commit_timeout: 50ms
  members:
    - id: gateway-1
      addr: "127.0.0.1:19101"
      raft_addr: "127.0.0.1:19201"
    - id: gateway-2
      addr: "127.0.0.1:19102"
      raft_addr: "127.0.0.1:19202"
    - id: gateway-3
      addr: "127.0.0.1:19103"
      raft_addr: "127.0.0.1:19203"
`

func TestRaftClusterConfigLoads(t *testing.T) {
	cfg, err := Parse([]byte(raftYAML))
	if err != nil {
		t.Fatalf("a consensus configuration does not load: %v", err)
	}
	if !cfg.Cluster.Raft.Enabled {
		t.Fatal("raft.enabled did not survive parsing")
	}
	if cfg.Cluster.Raft.ElectionTimeout != 500*time.Millisecond {
		t.Errorf("election_timeout = %s, want 500ms -- duration strings must parse", cfg.Cluster.Raft.ElectionTimeout)
	}
	if cfg.Cluster.Members[0].RaftAddr != "127.0.0.1:19201" {
		t.Errorf("raft_addr = %q", cfg.Cluster.Members[0].RaftAddr)
	}
}

// Consensus is off unless a file asks for it, so the command the case study
// publishes runs exactly as it did before there was a log at all.
func TestConsensusIsOffByDefault(t *testing.T) {
	if Defaults().Cluster.Raft.Enabled {
		t.Fatal("the defaults turn consensus on, which would change the published single-node command")
	}
	cfg, err := Load("../../configs/local.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.Raft.Enabled {
		t.Fatal("the shipped config turns consensus on")
	}
}

func TestBrokenConsensusConfigurationsAreRefused(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "consensus with nobody to reach consensus with",
			yaml: `
cluster:
  raft:
    enabled: true
`,
			want: "consensus needs the peers",
		},
		{
			// Falling back to the request address would have consensus try to
			// bind the port gRPC already holds: the node fails to start, and
			// the reason is two configuration lines apart from the symptom.
			name: "a member with no consensus address",
			yaml: `
cluster:
  raft:
    enabled: true
  members:
    - id: gateway-1
      addr: "127.0.0.1:19101"
    - id: gateway-2
      addr: "127.0.0.1:19102"
      raft_addr: "127.0.0.1:19202"
`,
			want: "cannot share the request port",
		},
		{
			name: "consensus on a port that already serves requests",
			yaml: `
cluster:
  raft:
    enabled: true
  members:
    - id: gateway-1
      addr: "127.0.0.1:19101"
      raft_addr: "127.0.0.1:19102"
    - id: gateway-2
      addr: "127.0.0.1:19102"
      raft_addr: "127.0.0.1:19202"
`,
			want: "already uses",
		},
		{
			name: "two members sharing one consensus address",
			yaml: `
cluster:
  raft:
    enabled: true
  members:
    - id: gateway-1
      addr: "127.0.0.1:19101"
      raft_addr: "127.0.0.1:19201"
    - id: gateway-2
      addr: "127.0.0.1:19102"
      raft_addr: "127.0.0.1:19201"
`,
			want: "already uses",
		},
		{
			name: "a negative election timeout",
			yaml: `
cluster:
  raft:
    enabled: true
    election_timeout: -1s
  members:
    - id: gateway-1
      addr: "127.0.0.1:19101"
      raft_addr: "127.0.0.1:19201"
`,
			want: "must not be negative",
		},
		{
			name: "a misspelled consensus key",
			yaml: `
cluster:
  raft:
    enabled: true
    hartbeat_timeout: 500ms
  members:
    - id: gateway-1
      addr: "127.0.0.1:19101"
      raft_addr: "127.0.0.1:19201"
`,
			want: "field hartbeat_timeout not found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatal("accepted a consensus configuration that could not work")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}
