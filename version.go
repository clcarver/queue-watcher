package main

import "fmt"

// These variables are set at build time via ldflags:
//   go build -ldflags "-X main.version=v1.2.3 -X main.commit=abc123 -X main.buildDate=2026-07-30"
var (
	version   = "0.3.2"
	commit    = "unknown"
	buildDate = "unknown"
)

// VersionString returns a human-readable version string.
func VersionString() string {
	return fmt.Sprintf("queue-watcher %s (commit=%s, built=%s)", version, commit, buildDate)
}
