package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"
)

// ── Laravel .env Reader ──

// EnvVars holds parsed key-value pairs from a Laravel .env file.
type EnvVars map[string]string

// ReadLaravelEnv parses the .env file in a Laravel project directory.
func ReadLaravelEnv(laravelPath string) (EnvVars, error) {
	envPath := filepath.Join(laravelPath, ".env")
	f, err := os.Open(envPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open .env: %w", err)
	}
	defer f.Close()

	vars := make(EnvVars)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		// Strip surrounding quotes.
		val = strings.Trim(val, `"'`)
		vars[key] = val
	}
	return vars, scanner.Err()
}

// BuildMSSQLDSN constructs a go-mssqldb connection string from Laravel env vars.
// Kept as utility for testing; primary path uses NewLaravelDB with SiteConfig.
func BuildMSSQLDSN(env EnvVars) string {
	host := env["DB_HOST"]
	if host == "" {
		host = "127.0.0.1"
	}
	port := env["DB_PORT"]
	if port == "" {
		port = "1433"
	}
	database := env["DB_DATABASE"]
	user := env["DB_USERNAME"]
	pass := env["DB_PASSWORD"]

	query := url.Values{}
	query.Add("database", database)
	query.Add("encrypt", "disable")
	query.Add("ApplicationIntent", "ReadOnly")

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, pass),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: query.Encode(),
	}
	return u.String()
}

// ── Laravel Database Reader ──

// QueueJob represents a pending/reserved job from the Laravel `jobs` table.
type QueueJob struct {
	ID          int64  `json:"id"`
	Queue       string `json:"queue"`
	PayloadName string `json:"payload_name"` // Extracted job class from payload JSON.
	Attempts    int    `json:"attempts"`
	ReservedAt  *int64 `json:"reserved_at"`  // Unix timestamp if reserved, nil if pending.
	AvailableAt int64  `json:"available_at"`
	CreatedAt   int64  `json:"created_at"`
}

// FailedJob represents a row from the Laravel `failed_jobs` table.
type FailedJob struct {
	ID         int64  `json:"id"`
	UUID       string `json:"uuid"`
	Connection string `json:"connection"`
	Queue      string `json:"queue"`
	JobName    string `json:"job_name"` // Extracted from payload.
	Exception  string `json:"exception"`
	FailedAt   string `json:"failed_at"`
}

// QueueMetrics holds a point-in-time snapshot of queue state.
type QueueMetrics struct {
	PendingCount  int            `json:"pending_count"`
	ReservedCount int            `json:"reserved_count"`
	FailedCount   int            `json:"failed_count"`
	PendingJobs   []QueueJob     `json:"pending_jobs"`
	ReservedJobs  []QueueJob     `json:"reserved_jobs"`
	RecentFailed  []FailedJob    `json:"recent_failed"`
	QueueDepths   map[string]int `json:"queue_depths"`
	QueueStats    []QueueStat    `json:"queue_stats,omitempty"`
}

// QueueStat holds per-queue timing averages.
type QueueStat struct {
	Queue        string  `json:"queue"`
	AvgWaitMs    float64 `json:"avg_wait_ms"`
	AvgExecMs    float64 `json:"avg_exec_ms"`
	Processed    int     `json:"processed"`
	Failed       int     `json:"failed"`
}

// trackedJob holds state for a job we saw in a previous poll.
type trackedJob struct {
	ID         int64
	Queue      string
	JobName    string
	CreatedAt  int64
	ReservedAt int64 // Unix timestamp when the worker picked it up.
	SeenAt     time.Time
}

// LaravelDB reads queue state from the Laravel application's MSSQL database.
type LaravelDB struct {
	db          *sql.DB
	mu          sync.RWMutex
	lastMetrics *QueueMetrics
	connected   bool
	lastError   string

	// Job-diff tracking: detect completions by comparing polls.
	prevJobs      map[int64]*trackedJob // Jobs seen in previous poll (by ID).
	prevFailedMax int64                 // Highest failed_jobs.id seen.
	OnCompletion  func(event JobEvent)  // Called when a completed/failed job is detected.
	OnNewFailed   func(job *FailedJob)  // Called when a new failed job is detected via DB diff.
}

// NewLaravelDB connects to the Laravel database using the site's .env file.
// The config fields (db_host_env, etc.) name keys inside the site's .env file.
func NewLaravelDB(sc *SiteConfig) (*LaravelDB, error) {
	env, err := ReadLaravelEnv(sc.LaravelPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read .env from %s: %w", sc.LaravelPath, err)
	}

	// Resolve each credential from the configured .env key name.
	// Fall back to standard Laravel keys if no mapping is configured.
	host := envLookup(env, sc.DBHostEnv, "DB_HOST")
	port := envLookup(env, sc.DBPortEnv, "DB_PORT")
	database := envLookup(env, sc.DBDatabaseEnv, "DB_DATABASE")
	user := envLookup(env, sc.DBUsernameEnv, "DB_USERNAME")
	pass := envLookup(env, sc.DBPasswordEnv, "DB_PASSWORD")

	if host == "" || database == "" {
		return nil, fmt.Errorf("database host and database name are required (check .env key mappings)")
	}
	if port == "" {
		port = "1433"
	}

	query := url.Values{}
	query.Add("database", database)
	query.Add("encrypt", "disable")
	query.Add("app name", "queue-watcher")

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, pass),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: query.Encode(),
	}
	dsn := u.String()

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MSSQL connection: %w", err)
	}

	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connectivity.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to MSSQL (%s): %w", host, err)
	}

	log.Printf("[laraveldb] Connected to MSSQL database %q at %s (read-only)", database, host)

	return &LaravelDB{
		db:        db,
		connected: true,
		prevJobs:  make(map[int64]*trackedJob),
	}, nil
}

// envLookup reads from the parsed .env using the configured key name,
// falling back to a default key if the configured name is empty.
func envLookup(env EnvVars, configuredKey, fallbackKey string) string {
	if configuredKey != "" {
		if v := env[configuredKey]; v != "" {
			return v
		}
	}
	return env[fallbackKey]
}

// Close closes the database connection.
func (ldb *LaravelDB) Close() {
	if ldb.db != nil {
		ldb.db.Close()
	}
}

// IsConnected returns whether the database connection is active.
func (ldb *LaravelDB) IsConnected() bool {
	ldb.mu.RLock()
	defer ldb.mu.RUnlock()
	return ldb.connected
}

// GetLastError returns the last connection/query error.
func (ldb *LaravelDB) GetLastError() string {
	ldb.mu.RLock()
	defer ldb.mu.RUnlock()
	return ldb.lastError
}

// PollMetrics queries the jobs and failed_jobs tables, detects completions
// by diffing against the previous poll, and caches the result.
func (ldb *LaravelDB) PollMetrics() (*QueueMetrics, error) {
	metrics := &QueueMetrics{
		QueueDepths: make(map[string]int),
	}

	// Query ALL jobs (pending + reserved) for the diff.
	allJobs, err := ldb.queryAllJobs()
	if err != nil {
		ldb.mu.Lock()
		ldb.lastError = err.Error()
		ldb.connected = false
		ldb.mu.Unlock()
		return nil, err
	}

	// Split into pending vs reserved and build current job map.
	currentJobs := make(map[int64]*trackedJob)
	now := time.Now()
	for i := range allJobs {
		j := &allJobs[i]
		tj := &trackedJob{
			ID:        j.ID,
			Queue:     j.Queue,
			JobName:   j.PayloadName,
			CreatedAt: j.CreatedAt,
			SeenAt:    now,
		}
		if j.ReservedAt != nil {
			tj.ReservedAt = *j.ReservedAt
			metrics.ReservedJobs = append(metrics.ReservedJobs, *j)
		} else {
			metrics.PendingJobs = append(metrics.PendingJobs, *j)
			metrics.QueueDepths[j.Queue]++
		}
		currentJobs[j.ID] = tj
	}
	metrics.PendingCount = len(metrics.PendingJobs)
	metrics.ReservedCount = len(metrics.ReservedJobs)

	// Query new failed jobs (only those above our high-water mark).
	failedJobs, newFailedIDs, maxFailedID, err := ldb.queryNewAndRecentFailed(ldb.prevFailedMax, 50)
	if err != nil {
		log.Printf("[laraveldb] Warning: failed to query failed_jobs: %v", err)
	} else {
		metrics.RecentFailed = failedJobs
		metrics.FailedCount = len(failedJobs)
	}

	// ── Diff against previous poll to detect completions ──
	if len(ldb.prevJobs) > 0 {
		for id, prev := range ldb.prevJobs {
			if _, stillExists := currentJobs[id]; stillExists {
				continue // Job still in the table — not done yet.
			}

			// Job disappeared from the `jobs` table.
			var status string
			if _, wasFailed := newFailedIDs[id]; wasFailed {
				status = "failed"
			} else {
				status = "processed"
			}

			// Calculate timing.
			var waitMs, durationMs int64
			if prev.ReservedAt > 0 {
				waitMs = (prev.ReservedAt - prev.CreatedAt) * 1000
				durationMs = now.Sub(prev.SeenAt).Milliseconds()
				// Approximate: we first saw it reserved at prev.SeenAt,
				// so duration ≈ time between that and now (when it disappeared).
			} else {
				waitMs = now.Sub(time.Unix(prev.CreatedAt, 0)).Milliseconds()
			}

			event := JobEvent{
				Timestamp:  now,
				JobID:      fmt.Sprintf("%d", id),
				JobName:    prev.JobName,
				Queue:      prev.Queue,
				Status:     status,
				WaitMs:     waitMs,
				DurationMs: durationMs,
			}

			if ldb.OnCompletion != nil {
				ldb.OnCompletion(event)
			}
		}
	}

	// Update previous state for next diff.
	ldb.prevJobs = currentJobs
	prevFailedMax := ldb.prevFailedMax
	if maxFailedID > ldb.prevFailedMax {
		// Fire OnNewFailed for each genuinely new failed job.
		if ldb.OnNewFailed != nil {
			for _, j := range failedJobs {
				if j.ID > prevFailedMax {
					jCopy := j
					ldb.OnNewFailed(&jCopy)
				}
			}
		}
		ldb.prevFailedMax = maxFailedID
	}

	ldb.mu.Lock()
	ldb.lastMetrics = metrics
	ldb.connected = true
	ldb.lastError = ""
	ldb.mu.Unlock()

	return metrics, nil
}

// GetCachedMetrics returns the most recent polled metrics without querying.
func (ldb *LaravelDB) GetCachedMetrics() *QueueMetrics {
	ldb.mu.RLock()
	defer ldb.mu.RUnlock()
	if ldb.lastMetrics == nil {
		return &QueueMetrics{QueueDepths: make(map[string]int)}
	}
	return ldb.lastMetrics
}

// queryAllJobs reads all rows from the `jobs` table (pending and reserved).
func (ldb *LaravelDB) queryAllJobs() ([]QueueJob, error) {
	query := `SELECT TOP 500 id, queue, payload, attempts, reserved_at, available_at, created_at
			  FROM jobs ORDER BY id ASC`

	rows, err := ldb.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []QueueJob
	for rows.Next() {
		var j QueueJob
		var payload string
		var reservedAt sql.NullInt64

		if err := rows.Scan(&j.ID, &j.Queue, &payload, &j.Attempts, &reservedAt, &j.AvailableAt, &j.CreatedAt); err != nil {
			continue
		}
		if reservedAt.Valid {
			j.ReservedAt = &reservedAt.Int64
		}
		j.PayloadName = extractJobName(payload)
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// queryNewAndRecentFailed reads from `failed_jobs`, returning:
// - The most recent N failed jobs (for display)
// - A set of IDs above the high-water mark (for diff detection)
// - The new high-water mark
func (ldb *LaravelDB) queryNewAndRecentFailed(prevMaxID int64, limit int) ([]FailedJob, map[int64]bool, int64, error) {
	query := fmt.Sprintf(`SELECT TOP %d id, ISNULL(uuid, ''), connection, queue, payload, exception, failed_at
						   FROM failed_jobs ORDER BY id DESC`, limit)

	rows, err := ldb.db.Query(query)
	if err != nil {
		return nil, nil, prevMaxID, fmt.Errorf("query failed_jobs: %w", err)
	}
	defer rows.Close()

	var jobs []FailedJob
	newIDs := make(map[int64]bool)
	maxID := prevMaxID

	for rows.Next() {
		var j FailedJob
		var payload string
		if err := rows.Scan(&j.ID, &j.UUID, &j.Connection, &j.Queue, &payload, &j.Exception, &j.FailedAt); err != nil {
			continue
		}
		j.JobName = extractJobName(payload)
		if len(j.Exception) > 1000 {
			j.Exception = j.Exception[:1000] + "..."
		}
		jobs = append(jobs, j)

		if j.ID > prevMaxID {
			newIDs[j.ID] = true
		}
		if j.ID > maxID {
			maxID = j.ID
		}
	}
	return jobs, newIDs, maxID, rows.Err()
}

// extractJobName parses the job class name from a Laravel job payload JSON.
func extractJobName(payload string) string {
	var p struct {
		DisplayName string `json:"displayName"`
		Job         string `json:"job"`
	}
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return "Unknown"
	}
	if p.DisplayName != "" {
		return p.DisplayName
	}
	if p.Job != "" {
		return p.Job
	}
	return "Unknown"
}

// ── SQLite Metrics Store (local retention) ──

// MetricsStore persists job events and throughput snapshots in a local SQLite database.
type MetricsStore struct {
	db *sql.DB
	mu sync.Mutex
}

// NewMetricsStore opens (or creates) a SQLite database for job history retention.
func NewMetricsStore(dataDir string) (*MetricsStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "metrics.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Enable WAL mode for concurrent reads.
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")

	store := &MetricsStore{db: db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
	}

	log.Printf("[metrics] SQLite store opened at %s", dbPath)
	return store, nil
}

// migrate creates tables if they don't exist.
func (ms *MetricsStore) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS job_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id TEXT NOT NULL,
		job_name TEXT NOT NULL,
		job_id TEXT,
		queue TEXT DEFAULT '',
		worker_id INTEGER,
		status TEXT NOT NULL,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		wait_ms INTEGER DEFAULT 0,
		duration_ms INTEGER DEFAULT 0
	);

	CREATE INDEX IF NOT EXISTS idx_job_events_site_ts ON job_events(site_id, timestamp);
	CREATE INDEX IF NOT EXISTS idx_job_events_status ON job_events(site_id, status);
	CREATE INDEX IF NOT EXISTS idx_job_events_queue ON job_events(site_id, queue);

	CREATE TABLE IF NOT EXISTS throughput_snapshots (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id TEXT NOT NULL,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		pending_count INTEGER DEFAULT 0,
		reserved_count INTEGER DEFAULT 0,
		processed_count INTEGER DEFAULT 0,
		failed_count INTEGER DEFAULT 0
	);

	CREATE INDEX IF NOT EXISTS idx_throughput_site_ts ON throughput_snapshots(site_id, timestamp);

	CREATE TABLE IF NOT EXISTS job_counters (
		site_id TEXT PRIMARY KEY,
		processed_count INTEGER NOT NULL DEFAULT 0,
		failed_count INTEGER NOT NULL DEFAULT 0,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := ms.db.Exec(schema)
	if err != nil {
		return err
	}

	// Add columns if upgrading from older schema.
	ms.db.Exec(`ALTER TABLE job_events ADD COLUMN queue TEXT DEFAULT ''`)
	ms.db.Exec(`ALTER TABLE job_events ADD COLUMN wait_ms INTEGER DEFAULT 0`)
	ms.db.Exec(`ALTER TABLE job_events ADD COLUMN duration_ms INTEGER DEFAULT 0`)

	return nil
}

// RecordJobEvent persists a job event (from DB diff or artisan output).
func (ms *MetricsStore) RecordJobEvent(siteID string, event JobEvent) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	_, err := ms.db.Exec(
		`INSERT INTO job_events (site_id, job_name, job_id, queue, worker_id, status, timestamp, wait_ms, duration_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		siteID, event.JobName, event.JobID, event.Queue, event.WorkerID,
		event.Status, event.Timestamp, event.WaitMs, event.DurationMs,
	)
	if err != nil {
		log.Printf("[metrics] Failed to record event: %v", err)
		return
	}

	switch event.Status {
	case "processed":
		_, err = ms.db.Exec(
			`INSERT INTO job_counters (site_id, processed_count, failed_count, updated_at)
			 VALUES (?, 1, 0, CURRENT_TIMESTAMP)
			 ON CONFLICT(site_id) DO UPDATE SET
			   processed_count = processed_count + 1,
			   updated_at = CURRENT_TIMESTAMP`,
			siteID,
		)
		if err != nil {
			log.Printf("[metrics] Failed to update processed counter: %v", err)
		}
	case "failed":
		_, err = ms.db.Exec(
			`INSERT INTO job_counters (site_id, processed_count, failed_count, updated_at)
			 VALUES (?, 0, 1, CURRENT_TIMESTAMP)
			 ON CONFLICT(site_id) DO UPDATE SET
			   failed_count = failed_count + 1,
			   updated_at = CURRENT_TIMESTAMP`,
			siteID,
		)
		if err != nil {
			log.Printf("[metrics] Failed to update failed counter: %v", err)
		}
	}
}

// RecordThroughputSnapshot saves a point-in-time queue depth measurement.
func (ms *MetricsStore) RecordThroughputSnapshot(siteID string, pending, reserved, processed, failed int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	_, err := ms.db.Exec(
		`INSERT INTO throughput_snapshots (site_id, pending_count, reserved_count, processed_count, failed_count)
		 VALUES (?, ?, ?, ?, ?)`,
		siteID, pending, reserved, processed, failed,
	)
	if err != nil {
		log.Printf("[metrics] Failed to record snapshot: %v", err)
	}
}

// GetPagedEvents returns a page of job events for a site, most recent first.
// Returns the events and the total count of events for pagination.
func (ms *MetricsStore) GetPagedEvents(siteID string, limit, offset int) ([]JobEvent, int) {
	var total int
	ms.db.QueryRow(`SELECT COUNT(*) FROM job_events WHERE site_id = ?`, siteID).Scan(&total)

	rows, err := ms.db.Query(
		`SELECT job_name, job_id, queue, worker_id, status, timestamp, wait_ms, duration_ms
		 FROM job_events WHERE site_id = ? ORDER BY id DESC LIMIT ? OFFSET ?`,
		siteID, limit, offset,
	)
	if err != nil {
		log.Printf("[metrics] Failed to query events: %v", err)
		return nil, total
	}
	defer rows.Close()

	var events []JobEvent
	for rows.Next() {
		var e JobEvent
		var ts string
		if err := rows.Scan(&e.JobName, &e.JobID, &e.Queue, &e.WorkerID, &e.Status, &ts, &e.WaitMs, &e.DurationMs); err != nil {
			continue
		}
		e.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
		if e.Timestamp.IsZero() {
			e.Timestamp, _ = time.Parse(time.RFC3339, ts)
		}
		events = append(events, e)
	}
	return events, total
}

// GetRecentEvents returns the most recent job events for a site.
func (ms *MetricsStore) GetRecentEvents(siteID string, limit int) []JobEvent {
	rows, err := ms.db.Query(
		`SELECT job_name, job_id, queue, worker_id, status, timestamp, wait_ms, duration_ms
		 FROM job_events WHERE site_id = ? ORDER BY id DESC LIMIT ?`,
		siteID, limit,
	)
	if err != nil {
		log.Printf("[metrics] Failed to query events: %v", err)
		return nil
	}
	defer rows.Close()

	var events []JobEvent
	for rows.Next() {
		var e JobEvent
		var ts string
		if err := rows.Scan(&e.JobName, &e.JobID, &e.Queue, &e.WorkerID, &e.Status, &ts, &e.WaitMs, &e.DurationMs); err != nil {
			continue
		}
		e.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
		if e.Timestamp.IsZero() {
			e.Timestamp, _ = time.Parse(time.RFC3339, ts)
		}
		events = append(events, e)
	}
	return events
}

// GetStats returns aggregate stats from the local store for a site.
func (ms *MetricsStore) GetStats(siteID string) JobStats {
	var stats JobStats

	err := ms.db.QueryRow(
		`SELECT processed_count, failed_count FROM job_counters WHERE site_id = ?`,
		siteID,
	).Scan(&stats.TotalProcessed, &stats.TotalFailed)
	if err == sql.ErrNoRows {
		// Backfill counters for existing installations that predate job_counters.
		ms.db.QueryRow(
			`SELECT COUNT(*) FROM job_events WHERE site_id = ? AND status = 'processed'`, siteID,
		).Scan(&stats.TotalProcessed)
		ms.db.QueryRow(
			`SELECT COUNT(*) FROM job_events WHERE site_id = ? AND status = 'failed'`, siteID,
		).Scan(&stats.TotalFailed)
		_, _ = ms.db.Exec(
			`INSERT INTO job_counters (site_id, processed_count, failed_count, updated_at)
			 VALUES (?, ?, ?, CURRENT_TIMESTAMP)
			 ON CONFLICT(site_id) DO UPDATE SET
			   processed_count = excluded.processed_count,
			   failed_count = excluded.failed_count,
			   updated_at = CURRENT_TIMESTAMP`,
			siteID, stats.TotalProcessed, stats.TotalFailed,
		)
	}

	return stats
}

// GetQueueStats returns per-queue average wait and execution times.
func (ms *MetricsStore) GetQueueStats(siteID string) []QueueStat {
	rows, err := ms.db.Query(
		`SELECT queue,
			AVG(CASE WHEN wait_ms > 0 THEN wait_ms END) as avg_wait,
			AVG(CASE WHEN duration_ms > 0 THEN duration_ms END) as avg_exec,
			SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END) as processed,
			SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
		 FROM job_events
		 WHERE site_id = ? AND status IN ('processed','failed') AND queue != ''
		 GROUP BY queue
		 ORDER BY processed DESC`,
		siteID,
	)
	if err != nil {
		log.Printf("[metrics] Failed to query queue stats: %v", err)
		return nil
	}
	defer rows.Close()

	var stats []QueueStat
	for rows.Next() {
		var s QueueStat
		var avgWait, avgExec sql.NullFloat64
		if err := rows.Scan(&s.Queue, &avgWait, &avgExec, &s.Processed, &s.Failed); err != nil {
			continue
		}
		if avgWait.Valid {
			s.AvgWaitMs = avgWait.Float64
		}
		if avgExec.Valid {
			s.AvgExecMs = avgExec.Float64
		}
		stats = append(stats, s)
	}
	return stats
}

// GetThroughputHistory returns throughput snapshots for charting.
func (ms *MetricsStore) GetThroughputHistory(siteID string, since time.Duration, limit int) []map[string]interface{} {
	cutoff := time.Now().Add(-since).Format("2006-01-02 15:04:05")
	rows, err := ms.db.Query(
		`SELECT timestamp, pending_count, reserved_count, processed_count, failed_count
		 FROM throughput_snapshots WHERE site_id = ? AND timestamp > ? ORDER BY id DESC LIMIT ?`,
		siteID, cutoff, limit,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var ts string
		var pending, reserved, processed, failed int
		if err := rows.Scan(&ts, &pending, &reserved, &processed, &failed); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"timestamp":       ts,
			"pending_count":   pending,
			"reserved_count":  reserved,
			"processed_count": processed,
			"failed_count":    failed,
		})
	}
	return results
}

// PruneByCount removes the oldest job_events rows, keeping at most maxRows per site.
// Call periodically to enforce a record-count limit rather than a time-based limit.
func (ms *MetricsStore) PruneByCount(siteID string, maxRows int) {
	if maxRows <= 0 {
		return
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.db.Exec(
		`DELETE FROM job_events WHERE site_id = ? AND id NOT IN (
			SELECT id FROM job_events WHERE site_id = ? ORDER BY id DESC LIMIT ?
		)`,
		siteID, siteID, maxRows,
	)
}

// GetFailedJobDetail returns a single failed job by ID, including the full exception.
func (ldb *LaravelDB) GetFailedJobDetail(id int64) (*FailedJob, error) {
	row := ldb.db.QueryRow(
		`SELECT id, ISNULL(uuid, ''), connection, queue, payload, exception, failed_at
		 FROM failed_jobs WHERE id = ?`, id)

	var j FailedJob
	var payload string
	if err := row.Scan(&j.ID, &j.UUID, &j.Connection, &j.Queue, &payload, &j.Exception, &j.FailedAt); err != nil {
		return nil, err
	}
	j.JobName = extractJobName(payload)
	return &j, nil
}

// DeleteFailedJob deletes a row from the failed_jobs table.
func (ldb *LaravelDB) DeleteFailedJob(uuid string) error {
	_, err := ldb.db.Exec(`DELETE FROM failed_jobs WHERE uuid = ?`, uuid)
	return err
}

// RetryFailedJob moves a failed job back to the jobs table (same as artisan queue:retry).
// It reads the payload from failed_jobs, inserts into jobs, then deletes from failed_jobs.
func (ldb *LaravelDB) RetryFailedJob(uuid string) error {
	// Read the failed job.
	row := ldb.db.QueryRow(
		`SELECT id, queue, payload FROM failed_jobs WHERE uuid = ?`, uuid)
	var id int64
	var queue, payload string
	if err := row.Scan(&id, &queue, &payload); err != nil {
		return fmt.Errorf("failed job %q not found: %w", uuid, err)
	}

	now := time.Now().Unix()

	// Insert back into jobs table.
	_, err := ldb.db.Exec(
		`INSERT INTO jobs (queue, payload, attempts, reserved_at, available_at, created_at)
		 VALUES (?, ?, 0, NULL, ?, ?)`,
		queue, payload, now, now,
	)
	if err != nil {
		return fmt.Errorf("failed to re-queue job: %w", err)
	}

	// Remove from failed_jobs.
	_, err = ldb.db.Exec(`DELETE FROM failed_jobs WHERE id = ?`, id)
	return err
}

// PruneOldData removes records older than the retention period.
func (ms *MetricsStore) PruneOldData(retention time.Duration) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	cutoff := time.Now().Add(-retention).Format("2006-01-02 15:04:05")
	ms.db.Exec(`DELETE FROM job_events WHERE timestamp < ?`, cutoff)
	ms.db.Exec(`DELETE FROM throughput_snapshots WHERE timestamp < ?`, cutoff)
}

// Close closes the SQLite database.
func (ms *MetricsStore) Close() {
	if ms.db != nil {
		ms.db.Close()
	}
}
