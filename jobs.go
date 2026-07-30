package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"time"
)

// JobEvent represents a single queue job lifecycle event parsed from worker output.
type JobEvent struct {
	Timestamp  time.Time `json:"timestamp"`
	JobID      string    `json:"job_id"`
	JobName    string    `json:"job_name"`
	Queue      string    `json:"queue"`
	Status     string    `json:"status"` // "processing", "processed", "failed"
	WorkerID   int       `json:"worker_id"`
	WaitMs     int64     `json:"wait_ms,omitempty"`     // Time spent waiting in queue.
	DurationMs int64     `json:"duration_ms,omitempty"` // Time spent executing.
}

// JobStats holds aggregate counters for the job history.
type JobStats struct {
	TotalProcessed int `json:"total_processed"`
	TotalFailed    int `json:"total_failed"`
	InFlight       int `json:"in_flight"`
}

// JobHistory is a thread-safe, fixed-size ring buffer of job events.
type JobHistory struct {
	mu       sync.RWMutex
	events   []JobEvent
	maxLen   int
	stats    JobStats
	inflight map[string]bool
}

// NewJobHistory creates a ring buffer that retains up to maxLen events.
func NewJobHistory(maxLen int) *JobHistory {
	return &JobHistory{
		events:   make([]JobEvent, 0, maxLen),
		maxLen:   maxLen,
		inflight: make(map[string]bool),
	}
}

// Add appends an event, evicting the oldest if the buffer is full.
func (jh *JobHistory) Add(event JobEvent) {
	jh.mu.Lock()
	defer jh.mu.Unlock()

	switch event.Status {
	case "processing":
		jh.inflight[event.JobID] = true
	case "processed":
		jh.stats.TotalProcessed++
		delete(jh.inflight, event.JobID)
	case "failed":
		jh.stats.TotalFailed++
		delete(jh.inflight, event.JobID)
	}

	if len(jh.events) >= jh.maxLen {
		jh.events = jh.events[1:]
	}
	jh.events = append(jh.events, event)
}

// Recent returns the last n events, most recent first.
func (jh *JobHistory) Recent(n int) []JobEvent {
	jh.mu.RLock()
	defer jh.mu.RUnlock()

	total := len(jh.events)
	if n > total {
		n = total
	}

	result := make([]JobEvent, n)
	for i := 0; i < n; i++ {
		result[i] = jh.events[total-1-i]
	}
	return result
}

// GetStats returns a snapshot of aggregate job statistics.
func (jh *JobHistory) GetStats() JobStats {
	jh.mu.RLock()
	defer jh.mu.RUnlock()

	stats := jh.stats
	stats.InFlight = len(jh.inflight)
	return stats
}

// ── Artisan Output Parsing ──

// telemetryPayload matches the JSON shape emitted by the AppServiceProvider.
//
//	{"telemetry":true,"status":"reserved|completed|failed","job_id":"123",
//	 "job_name":"App\\Actions\\SyncMoveOrders","error":null,
//	 "time":"2026-07-30T10:00:00-04:00"}
type telemetryPayload struct {
	Telemetry bool    `json:"telemetry"`
	Status    string  `json:"status"`
	JobID     string  `json:"job_id"`
	JobName   string  `json:"job_name"`
	Error     *string `json:"error"`
	Time      string  `json:"time"`
}

// parseTelemetryLine tries to decode a JSON telemetry line from the
// AppServiceProvider. Returns a JobEvent and true on success.
func parseTelemetryLine(line string, workerID int) (JobEvent, bool) {
	// Quick guard: telemetry lines always start with '{' and contain "telemetry".
	if len(line) == 0 || line[0] != '{' {
		return JobEvent{}, false
	}

	var tp telemetryPayload
	if err := json.Unmarshal([]byte(line), &tp); err != nil || !tp.Telemetry {
		return JobEvent{}, false
	}

	ts, _ := time.Parse(time.RFC3339, tp.Time)
	if ts.IsZero() {
		ts = time.Now()
	}

	// Map Laravel event names → internal status.
	status := tp.Status
	switch status {
	case "reserved":
		status = "processing"
	case "completed":
		status = "processed"
	case "failed":
		// stays "failed"
	}

	return JobEvent{
		Timestamp: ts,
		JobID:     tp.JobID,
		JobName:   tp.JobName,
		Status:    status,
		WorkerID:  workerID,
	}, true
}

// Classic format (Laravel 8/9):
//
//	[2024-01-15 10:30:00][abc123] Processing: App\Jobs\SendEmail
//	[2024-01-15 10:30:01][abc123] Processed:  App\Jobs\SendEmail
var artisanClassicRegex = regexp.MustCompile(
	`\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\[([^\]]*)\]\s*(Processing|Processed|Failed):\s*(.+)`,
)

// Laravel 10+ format:
//
//	2024-01-15 10:30:00 App\Jobs\SendEmail ............. RUNNING
//	2024-01-15 10:30:01 App\Jobs\SendEmail ............. 1.23s DONE
//	2024-01-15 10:30:02 App\Jobs\SendEmail ............. FAIL
var artisanModernRegex = regexp.MustCompile(
	`(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+([\w\\]+(?:\\[\w]+)+)\s+\.{2,}\s+(?:[\d.]+s\s+)?(RUNNING|DONE|FAIL)`,
)

// Simple "Processing:" pattern as a fallback (catches partial/custom formats).
var artisanFallbackRegex = regexp.MustCompile(
	`(Processing|Processed|Failed):\s+([\w\\]+(?:\\[\w]+)*)`,
)

// ParseArtisanLine attempts to parse a queue worker output line into a JobEvent.
// Priority: JSON telemetry → classic regex → modern regex → fallback regex.
func ParseArtisanLine(line string, workerID int) (JobEvent, bool) {
	// 1. Try structured JSON telemetry (from AppServiceProvider).
	if event, ok := parseTelemetryLine(line, workerID); ok {
		return event, true
	}

	// 2. Try classic format.
	if m := artisanClassicRegex.FindStringSubmatch(line); m != nil {
		ts, _ := time.Parse("2006-01-02 15:04:05", m[1])
		if ts.IsZero() {
			ts = time.Now()
		}
		return JobEvent{
			Timestamp: ts,
			JobID:     m[2],
			JobName:   strings.TrimSpace(m[4]),
			Status:    normalizeStatus(m[3]),
			WorkerID:  workerID,
		}, true
	}

	// 3. Try Laravel 10+ format.
	if m := artisanModernRegex.FindStringSubmatch(line); m != nil {
		ts, _ := time.Parse("2006-01-02 15:04:05", m[1])
		if ts.IsZero() {
			ts = time.Now()
		}
		status := "processing"
		switch m[3] {
		case "DONE":
			status = "processed"
		case "FAIL":
			status = "failed"
		case "RUNNING":
			status = "processing"
		}
		return JobEvent{
			Timestamp: ts,
			JobName:   m[2],
			Status:    status,
			WorkerID:  workerID,
		}, true
	}

	// 4. Fallback: keywords only.
	if m := artisanFallbackRegex.FindStringSubmatch(line); m != nil {
		return JobEvent{
			Timestamp: time.Now(),
			JobName:   m[2],
			Status:    normalizeStatus(m[1]),
			WorkerID:  workerID,
		}, true
	}

	return JobEvent{}, false
}

func normalizeStatus(s string) string {
	switch s {
	case "Processing":
		return "processing"
	case "Processed":
		return "processed"
	case "Failed":
		return "failed"
	default:
		return strings.ToLower(s)
	}
}

// ── lineWriter: io.Writer that parses artisan output line-by-line ──
// This avoids the StdoutPipe + cmd.Wait() race condition by processing
// output synchronously as the child process writes it.

type lineWriter struct {
	buf      []byte
	workerID int
	onEvent  func(JobEvent) // Called for each parsed job event.
}

// Write implements io.Writer. It buffers partial lines and emits complete ones.
func (lw *lineWriter) Write(p []byte) (n int, err error) {
	lw.buf = append(lw.buf, p...)

	for {
		idx := bytes.IndexByte(lw.buf, '\n')
		if idx < 0 {
			break
		}

		line := string(lw.buf[:idx])
		lw.buf = lw.buf[idx+1:]

		// Strip \r for Windows line endings.
		line = strings.TrimRight(line, "\r")

		if event, ok := ParseArtisanLine(line, lw.workerID); ok {
			if lw.onEvent != nil {
				lw.onEvent(event)
			}
		}
	}

	return len(p), nil
}
