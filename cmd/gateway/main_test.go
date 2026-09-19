package main

import "testing"

// Both argument forms have to work. Everything declarative -- compose, a
// Kubernetes manifest, a systemd unit -- writes `--flag=value`, while a test
// harness or a shell loop usually writes `--flag value`. A binary that accepts
// only the form its own tests use passes every suite and fails every deployment.
func TestParseFlagsAcceptsBothArgumentForms(t *testing.T) {
	cases := [][]string{
		{"--node", "gateway-2", "--config", "./configs/local.yaml"},
		{"--node=gateway-2", "--config=./configs/local.yaml"},
		{"-node", "gateway-2", "-config", "./configs/local.yaml"},
	}
	for _, args := range cases {
		opts, err := parseFlags(args)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if opts.node != "gateway-2" {
			t.Errorf("%v: node = %q, want gateway-2", args, opts.node)
		}
		if opts.configPath != "./configs/local.yaml" {
			t.Errorf("%v: config = %q", args, opts.configPath)
		}
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	opts, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.logLevel != "info" || opts.logFormat != "text" {
		t.Errorf("defaults = %+v, want info/text", opts)
	}
}

func TestParseFlagsRejectsJunk(t *testing.T) {
	if _, err := parseFlags([]string{"--node", "gateway-1", "extra"}); err == nil {
		t.Error("a stray positional argument was accepted")
	}
	if _, err := parseFlags([]string{"--nope"}); err == nil {
		t.Error("an unknown flag was accepted")
	}
}

func TestLoggerRejectsUnknownLevels(t *testing.T) {
	if _, err := newLogger("verbose", "text"); err == nil {
		t.Error("--log-level verbose was accepted")
	}
	if _, err := newLogger("info", "xml"); err == nil {
		t.Error("--log-format xml was accepted")
	}
	if _, err := newLogger("debug", "json"); err != nil {
		t.Errorf("debug/json rejected: %v", err)
	}
}
