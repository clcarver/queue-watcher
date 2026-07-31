package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// LogLevel represents the severity of a log entry.
type LogLevel string

const (
	LogLevelDebug     LogLevel = "DEBUG"
	LogLevelInfo      LogLevel = "INFO"
	LogLevelNotice    LogLevel = "NOTICE"
	LogLevelWarning   LogLevel = "WARNING"
	LogLevelError     LogLevel = "ERROR"
	LogLevelCritical  LogLevel = "CRITICAL"
	LogLevelAlert     LogLevel = "ALERT"
	LogLevelEmergency LogLevel = "EMERGENCY"
)

// LogEntry represents a single parsed Laravel log entry.
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     LogLevel  `json:"level"`
	Channel   string    `json:"channel"`
	Message   string    `json:"message"`
	Context   string    `json:"context,omitempty"`  // JSON context blob
	Extra     string    `json:"extra,omitempty"`    // JSON extra blob (stack trace etc.)
	Raw       string    `json:"raw"`
}

// LogFile describes a log file available for viewing.
type LogFile struct {
	Name     string    `json:"name"`
	Path     string    `json:"path"`
	SizeBytes int64    `json:"size_bytes"`
	Modified time.Time `json:"modified"`
}

// LogEntryPage is a paginated response of log entries.
type LogEntryPage struct {
	Entries    []LogEntry `json:"entries"`
	Total      int        `json:"total"`
	Page       int        `json:"page"`
	TotalPages int        `json:"total_pages"`
	File       string     `json:"file"`
}

// monologRegex matches a standard Laravel/Monolog log line:
// [2024-01-15 10:30:00] local.ERROR: Some message here {"context":"value"} {"extra":"value"}
var monologRegex = regexp.MustCompile(
	`^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)?)\] (\w+)\.(\w+): (.*)$`,
)

// ListLogFiles returns all .log files inside the Laravel storage/logs directory.
func ListLogFiles(laravelPath string) ([]LogFile, error) {
	logsDir := filepath.Join(laravelPath, "storage", "logs")
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read logs directory: %w", err)
	}

	var files []LogFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, LogFile{
			Name:      e.Name(),
			Path:      filepath.Join(logsDir, e.Name()),
			SizeBytes: info.Size(),
			Modified:  info.ModTime(),
		})
	}

	// Sort newest first.
	sort.Slice(files, func(i, j int) bool {
		return files[i].Modified.After(files[j].Modified)
	})

	return files, nil
}

// ReadLogEntries reads and parses a log file, returning a filtered+paginated result.
// filterLevel: empty = all levels. search: empty = no filter.
func ReadLogEntries(filePath string, page, limit int, filterLevel, search string) (*LogEntryPage, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open log file: %w", err)
	}
	defer f.Close()

	// Parse all entries first so we can paginate correctly.
	var all []LogEntry
	scanner := bufio.NewScanner(f)
	// Increase buffer for large log lines (stack traces can be huge).
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 4*1024*1024)

	var current *LogEntry

	flush := func() {
		if current == nil {
			return
		}
		// Apply filters before appending.
		if filterLevel != "" && !strings.EqualFold(string(current.Level), filterLevel) {
			current = nil
			return
		}
		if search != "" {
			needle := strings.ToLower(search)
			if !strings.Contains(strings.ToLower(current.Message), needle) &&
				!strings.Contains(strings.ToLower(current.Context), needle) &&
				!strings.Contains(strings.ToLower(current.Extra), needle) {
				current = nil
				return
			}
		}
		all = append(all, *current)
		current = nil
	}

	for scanner.Scan() {
		line := scanner.Text()

		if m := monologRegex.FindStringSubmatch(line); m != nil {
			flush()

			ts := parseLogTimestamp(m[1])
			rest := strings.TrimSpace(m[4])

			// Split trailing JSON blobs: message {"context"} {"extra"}
			msg, ctx, extra := splitMessageContextExtra(rest)

			current = &LogEntry{
				Timestamp: ts,
				Channel:   m[2],
				Level:     normalizeLevel(m[3]),
				Message:   msg,
				Context:   ctx,
				Extra:     extra,
				Raw:       line,
			}
		} else if current != nil {
			// Continuation line (stack trace etc.) — append to extra.
			current.Extra += "\n" + line
			current.Raw += "\n" + line
		}
		// Lines before any entry are silently dropped.
	}
	flush()

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading log file: %w", err)
	}

	// Reverse so newest is first.
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}

	total := len(all)
	totalPages := (total + limit - 1) / limit
	if totalPages < 1 {
		totalPages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * limit
	end := start + limit
	if end > total {
		end = total
	}

	var pageEntries []LogEntry
	if start < total {
		pageEntries = all[start:end]
	}

	return &LogEntryPage{
		Entries:    pageEntries,
		Total:      total,
		Page:       page,
		TotalPages: totalPages,
		File:       filepath.Base(filePath),
	}, nil
}

// parseLogTimestamp tries several timestamp formats used by Monolog.
func parseLogTimestamp(s string) time.Time {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// splitMessageContextExtra separates the message text from trailing JSON blobs.
// Laravel logs end with: message {context_json} {extra_json}
func splitMessageContextExtra(rest string) (msg, ctx, extra string) {
	// Walk backwards to find up to two trailing JSON objects.
	rest = strings.TrimSpace(rest)

	var blobs []string
	for {
		idx := strings.LastIndex(rest, " {")
		if idx < 0 {
			break
		}
		candidate := strings.TrimSpace(rest[idx:])
		// Quick check: must end with }
		if !strings.HasSuffix(candidate, "}") {
			break
		}
		blobs = append([]string{candidate}, blobs...)
		rest = strings.TrimSpace(rest[:idx])
		if len(blobs) >= 2 {
			break
		}
	}

	msg = rest
	switch len(blobs) {
	case 1:
		ctx = blobs[0]
	case 2:
		ctx = blobs[0]
		extra = blobs[1]
	}
	return
}

// normalizeLevel maps Monolog level strings to our canonical LogLevel constants.
func normalizeLevel(s string) LogLevel {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return LogLevelDebug
	case "INFO":
		return LogLevelInfo
	case "NOTICE":
		return LogLevelNotice
	case "WARNING":
		return LogLevelWarning
	case "ERROR":
		return LogLevelError
	case "CRITICAL":
		return LogLevelCritical
	case "ALERT":
		return LogLevelAlert
	case "EMERGENCY":
		return LogLevelEmergency
	default:
		return LogLevel(strings.ToUpper(s))
	}
}

// IsErrorLevel returns true for WARNING and above.
func IsErrorLevel(level LogLevel) bool {
	switch level {
	case LogLevelWarning, LogLevelError, LogLevelCritical, LogLevelAlert, LogLevelEmergency:
		return true
	}
	return false
}
