package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerStatus represents the current state of a single queue worker process.
type WorkerStatus struct {
	ID              int       `json:"id"`
	PID             int       `json:"pid"`
	Status          string    `json:"status"`
	StartedAt       time.Time `json:"started_at"`
	RestartCount    int       `json:"restart_count"`
	LastExitCode    int       `json:"last_exit_code"`
	Uptime          string    `json:"uptime"`
	FaultReason     string    `json:"fault_reason,omitempty"`
	CurrentJob      string    `json:"current_job,omitempty"`
	Label           string    `json:"label"`
	QueueConnection string    `json:"queue_connection"`
	QueueNames      string    `json:"queue_names"`
	MaxMemoryMB     int       `json:"max_memory_mb"`
	TimeoutSeconds  int       `json:"timeout_seconds"`
	MaxJobs         int       `json:"max_jobs"`
	Tries           int       `json:"tries"`
}

// Worker represents a single managed queue worker process.
type Worker struct {
	ID           int
	Config       *WorkerConfig // Per-worker artisan settings.
	cmd          *exec.Cmd
	cancel       context.CancelFunc
	ctx          context.Context
	StartedAt    time.Time
	RestartCount int
	LastExitCode int
	Running      bool
	Stopped      bool
	Deleted      bool
	Faulted      bool
	FaultReason  string
	CurrentJob   string
	mu           sync.Mutex
}

const (
	// rapidFailThreshold is how many consecutive rapid failures trip the circuit breaker.
	rapidFailThreshold = 5
	// rapidFailWindow is the max process lifetime considered a "rapid" failure.
	rapidFailWindow = 10 * time.Second
	// maxBackoff caps the exponential restart delay.
	maxBackoff = 60 * time.Second
)

// WorkerManager supervises all queue worker processes with self-healing capabilities.
type WorkerManager struct {
	cfg        *Config
	workers    []*Worker
	mu         sync.RWMutex
	ctx        context.Context
	nextID     atomic.Int32
	stopCh     chan struct{}
	jobs       *JobHistory
	OnJobEvent func(JobEvent) // Optional callback when a job event is detected.

	// jobTimings tracks when each job started processing (for duration calculation).
	jobTimings   map[string]time.Time
	jobTimingsMu sync.Mutex
}

// NewWorkerManager creates a new worker manager instance.
func NewWorkerManager(cfg *Config, ctx context.Context) *WorkerManager {
	wm := &WorkerManager{
		cfg:        cfg,
		workers:    make([]*Worker, 0),
		ctx:        ctx,
		stopCh:     make(chan struct{}),
		jobs:       NewJobHistory(500),
		jobTimings: make(map[string]time.Time),
	}
	return wm
}

// GetJobHistory returns the job history ring buffer for this manager.
func (wm *WorkerManager) GetJobHistory() *JobHistory {
	return wm.jobs
}

// Run starts the configured number of workers and monitors them.
func (wm *WorkerManager) Run() {
	// ── Pre-flight checks ──
	if _, err := exec.LookPath(wm.cfg.PHPBinary); err != nil {
		log.Printf("[manager] WARNING: PHP binary %q not found on PATH: %v", wm.cfg.PHPBinary, err)
		log.Printf("[manager] Workers will fail to start. Verify php_binary in queue-watcher.json.")
	}
	if info, err := os.Stat(wm.cfg.LaravelPath); err != nil || !info.IsDir() {
		log.Printf("[manager] WARNING: Laravel path %q is not accessible: %v", wm.cfg.LaravelPath, err)
		log.Printf("[manager] Workers will fail. Verify laravel_path in queue-watcher.json.")
	}
	artisanPath := filepath.Join(wm.cfg.LaravelPath, "artisan")
	if _, err := os.Stat(artisanPath); err != nil {
		log.Printf("[manager] WARNING: artisan file not found at %q: %v", artisanPath, err)
	}

	log.Printf("[manager] Pre-flight checks complete.")

	// Workers are spawned externally by SiteManager with individual configs.
	// Block until context is cancelled.
	<-wm.ctx.Done()
	log.Println("[manager] Context cancelled, manager shutting down.")
}

// SpawnWorkerWithConfig creates and starts a new worker with individual settings.
// Returns the worker ID assigned.
func (wm *WorkerManager) SpawnWorkerWithConfig(wcfg *WorkerConfig) int {
	id := int(wm.nextID.Add(1))

	// Apply defaults for any zero-value fields.
	if wcfg.QueueConnection == "" {
		wcfg.QueueConnection = "redis"
	}
	if wcfg.QueueNames == "" {
		wcfg.QueueNames = "default"
	}
	if wcfg.MaxMemoryMB <= 0 {
		wcfg.MaxMemoryMB = 128
	}
	if wcfg.TimeoutSeconds <= 0 {
		wcfg.TimeoutSeconds = 60
	}
	if wcfg.Tries <= 0 {
		wcfg.Tries = 3
	}
	if wcfg.Label == "" {
		wcfg.Label = fmt.Sprintf("%s:%s", wcfg.QueueConnection, wcfg.QueueNames)
	}

	worker := &Worker{
		ID:      id,
		Config:  wcfg,
		Running: false,
	}

	wm.mu.Lock()
	wm.workers = append(wm.workers, worker)
	wm.mu.Unlock()

	go wm.supervise(worker)

	log.Printf("[manager] Worker #%d spawned (%s queue=%s).", id, wcfg.Label, wcfg.QueueNames)
	return id
}

// EditWorker updates a worker's config, stops it, and restarts with new settings.
func (wm *WorkerManager) EditWorker(id int, newCfg *WorkerConfig) bool {
	wm.mu.RLock()
	var target *Worker
	for _, w := range wm.workers {
		if w.ID == id {
			target = w
			break
		}
	}
	wm.mu.RUnlock()

	if target == nil {
		return false
	}

	// Stop the current process.
	target.mu.Lock()
	target.Stopped = true
	if target.cancel != nil {
		target.cancel()
	}
	target.mu.Unlock()

	// Wait for it to exit.
	time.Sleep(1 * time.Second)

	// Remove the old worker.
	wm.removeWorker(id)

	// Spawn a fresh one with new config.
	wm.SpawnWorkerWithConfig(newCfg)
	return true
}

// StopWorker gracefully terminates a specific worker by ID but keeps it in the list.
func (wm *WorkerManager) StopWorker(id int) bool {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	for _, w := range wm.workers {
		if w.ID == id {
			w.mu.Lock()
			w.Stopped = true
			if w.cancel != nil {
				w.cancel()
			}
			w.mu.Unlock()

			log.Printf("[manager] Worker #%d stopped by user.", id)
			return true
		}
	}
	return false
}

// DeleteWorker terminates a worker (if running) and removes it from the managed list entirely.
func (wm *WorkerManager) DeleteWorker(id int) bool {
	wm.mu.RLock()
	var target *Worker
	for _, w := range wm.workers {
		if w.ID == id {
			target = w
			break
		}
	}
	wm.mu.RUnlock()

	if target == nil {
		return false
	}

	// Signal the worker to stop and mark for deletion.
	target.mu.Lock()
	target.Stopped = true
	target.Deleted = true
	if target.cancel != nil {
		target.cancel()
	}
	target.mu.Unlock()

	// Remove from the managed list.
	wm.removeWorker(id)
	log.Printf("[manager] Worker #%d deleted by user.", id)
	return true
}

// StopAll gracefully terminates all active workers and waits for them to exit.
func (wm *WorkerManager) StopAll() {
	log.Println("[manager] Stopping all workers...")

	wm.mu.RLock()
	workers := make([]*Worker, len(wm.workers))
	copy(workers, wm.workers)
	wm.mu.RUnlock()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(worker *Worker) {
			defer wg.Done()
			worker.mu.Lock()
			if worker.cancel != nil {
				worker.cancel()
			}
			worker.mu.Unlock()
		}(w)
	}

	// Wait for all cancel signals to be sent, with timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[manager] All workers signaled to stop.")
	case <-time.After(30 * time.Second):
		log.Println("[manager] Timeout waiting for workers to stop.")
	}
}

// RestartAll performs a graceful hot-reload: stops all workers then spawns fresh ones
// with the same individual configs.
func (wm *WorkerManager) RestartAll() {
	log.Println("[manager] Hot-reload: restarting all workers...")

	wm.mu.RLock()
	workers := make([]*Worker, len(wm.workers))
	copy(workers, wm.workers)
	// Save configs before stopping.
	configs := make([]*WorkerConfig, 0, len(workers))
	for _, w := range workers {
		if w.Config != nil {
			cfg := *w.Config
			configs = append(configs, &cfg)
		}
	}
	wm.mu.RUnlock()

	// Mark workers as intentionally stopped so their supervise loops do not auto-restart
	// after a graceful queue:restart exit.
	for _, w := range workers {
		w.mu.Lock()
		w.Stopped = true
		w.mu.Unlock()
	}

	// Ask Laravel workers to gracefully stop after their current job.
	if err := wm.triggerQueueRestart(); err != nil {
		log.Printf("[manager] queue:restart failed; falling back to immediate cancellation: %v", err)
	}

	// Wait for workers to exit naturally; force-cancel only if they take too long.
	deadline := time.Now().Add(90 * time.Second)
	for {
		allExited := true
		for _, w := range workers {
			w.mu.Lock()
			running := w.Running
			w.mu.Unlock()
			if running {
				allExited = false
				break
			}
		}

		if allExited {
			break
		}
		if time.Now().After(deadline) {
			log.Println("[manager] Graceful restart timeout reached; force-stopping remaining workers.")
			for _, w := range workers {
				w.mu.Lock()
				if w.Running && w.cancel != nil {
					w.cancel()
				}
				w.mu.Unlock()
			}
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Clear the workers slice.
	wm.mu.Lock()
	wm.workers = make([]*Worker, 0)
	wm.mu.Unlock()

	// Spawn fresh workers with their original configs.
	for _, cfg := range configs {
		wm.SpawnWorkerWithConfig(cfg)
	}

	log.Printf("[manager] Hot-reload complete. %d fresh workers spawned.", len(configs))
}

// triggerQueueRestart requests a graceful worker restart using Laravel's
// built-in queue:restart mechanism.
func (wm *WorkerManager) triggerQueueRestart() error {
	cmd := exec.CommandContext(wm.ctx, wm.cfg.PHPBinary, "artisan", "queue:restart")
	cmd.Dir = wm.cfg.LaravelPath
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("queue:restart failed: %w (output: %s)", err, string(output))
	}
	log.Printf("[manager] queue:restart issued successfully: %s", string(output))
	return nil
}

// GetStatuses returns thread-safe snapshot of all worker statuses.
func (wm *WorkerManager) GetStatuses() []WorkerStatus {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	statuses := make([]WorkerStatus, 0, len(wm.workers))
	for _, w := range wm.workers {
		w.mu.Lock()
		status := WorkerStatus{
			ID:           w.ID,
			RestartCount: w.RestartCount,
			LastExitCode: w.LastExitCode,
		}
		if w.Config != nil {
			status.Label = w.Config.Label
			status.QueueConnection = w.Config.QueueConnection
			status.QueueNames = w.Config.QueueNames
			status.MaxMemoryMB = w.Config.MaxMemoryMB
			status.TimeoutSeconds = w.Config.TimeoutSeconds
			status.MaxJobs = w.Config.MaxJobs
			status.Tries = w.Config.Tries
		}
		if w.Faulted {
			status.Status = "faulted"
			status.Uptime = "—"
			status.FaultReason = w.FaultReason
		} else if w.Running {
			status.Status = "running"
			status.StartedAt = w.StartedAt
			status.Uptime = time.Since(w.StartedAt).Truncate(time.Second).String()
			status.CurrentJob = w.CurrentJob
			if w.cmd != nil && w.cmd.Process != nil {
				status.PID = w.cmd.Process.Pid
			}
		} else {
			status.Status = "stopped"
			status.Uptime = "—"
		}
		w.mu.Unlock()
		statuses = append(statuses, status)
	}
	return statuses
}

// GetWorkerCount returns the current number of managed workers.
func (wm *WorkerManager) GetWorkerCount() int {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return len(wm.workers)
}

// supervise is the main loop for a single worker — restarts on exit with
// exponential backoff and a circuit breaker that trips after repeated rapid failures.
func (wm *WorkerManager) supervise(worker *Worker) {
	var consecutiveRapidFails int

	for {
		// Check if the application context is done.
		select {
		case <-wm.ctx.Done():
			return
		default:
		}

		// Create a cancellable context for this specific worker run.
		workerCtx, workerCancel := context.WithCancel(wm.ctx)

		worker.mu.Lock()
		worker.ctx = workerCtx
		worker.cancel = workerCancel
		worker.mu.Unlock()

		// Build the artisan command using this worker's individual config.
		args := wm.buildArtisanArgs(worker.Config)
		cmd := exec.CommandContext(workerCtx, wm.cfg.PHPBinary, args...)
		cmd.Dir = wm.cfg.LaravelPath

		worker.mu.Lock()
		worker.cmd = cmd
		worker.StartedAt = time.Now()
		worker.Running = true
		worker.mu.Unlock()

		spawnTime := time.Now()
		log.Printf("[worker #%d] Starting: %s %v", worker.ID, wm.cfg.PHPBinary, args)

		// Use a lineWriter to capture and parse artisan output inline.
		// This avoids the StdoutPipe + Wait() race condition.
		outputWriter := &lineWriter{
			workerID: worker.ID,
			onEvent: func(event JobEvent) {
				// Compute execution duration from reserved→completed timing.
				if event.JobID != "" {
					switch event.Status {
					case "processing":
						wm.jobTimingsMu.Lock()
						wm.jobTimings[event.JobID] = event.Timestamp
						wm.jobTimingsMu.Unlock()
					case "processed", "failed":
						wm.jobTimingsMu.Lock()
						if startTime, ok := wm.jobTimings[event.JobID]; ok {
							event.DurationMs = event.Timestamp.Sub(startTime).Milliseconds()
							delete(wm.jobTimings, event.JobID)
						}
						wm.jobTimingsMu.Unlock()
					}
				}

				wm.jobs.Add(event)
				// Notify external listeners (e.g., metrics store).
				if wm.OnJobEvent != nil {
					wm.OnJobEvent(event)
				}
				// Update the worker's current job state.
				worker.mu.Lock()
				switch event.Status {
				case "processing":
					worker.CurrentJob = event.JobName
				case "processed", "failed":
					worker.CurrentJob = ""
				}
				worker.mu.Unlock()
			},
		}
		cmd.Stdout = outputWriter
		cmd.Stderr = outputWriter

		// Attempt to start the process.
		err := cmd.Start()
		if err != nil {
			log.Printf("[worker #%d] Failed to start: %v", worker.ID, err)
			worker.mu.Lock()
			worker.Running = false
			worker.LastExitCode = -1
			worker.mu.Unlock()
		} else {
			// Wait for the process to exit.
			waitErr := cmd.Wait()

			worker.mu.Lock()
			worker.Running = false
			if cmd.ProcessState != nil {
				worker.LastExitCode = cmd.ProcessState.ExitCode()
			}
			worker.mu.Unlock()

			if waitErr != nil {
				log.Printf("[worker #%d] Exited with error: %v", worker.ID, waitErr)
			} else {
				log.Printf("[worker #%d] Exited cleanly (code 0).", worker.ID)
			}
		}

		processLifetime := time.Since(spawnTime)

		// ── Guard: should we restart? ──

		// App shutting down.
		select {
		case <-wm.ctx.Done():
			log.Printf("[worker #%d] Not restarting (shutdown in progress).", worker.ID)
			return
		default:
		}

		// Intentionally stopped or deleted by user.
		worker.mu.Lock()
		stopped := worker.Stopped
		deleted := worker.Deleted
		worker.mu.Unlock()

		if stopped || deleted {
			log.Printf("[worker #%d] Intentionally stopped, not restarting.", worker.ID)
			return
		}

		// Worker context cancelled (e.g., during hot-reload).
		select {
		case <-workerCtx.Done():
			if wm.ctx.Err() == nil {
				log.Printf("[worker #%d] Context cancelled, not restarting.", worker.ID)
				return
			}
			return
		default:
		}

		// ── Circuit breaker: detect rapid failure loops ──

		if processLifetime < rapidFailWindow {
			consecutiveRapidFails++
		} else {
			// Process ran long enough — reset the counter.
			consecutiveRapidFails = 0
		}

		if consecutiveRapidFails >= rapidFailThreshold {
			reason := fmt.Sprintf(
				"Circuit breaker tripped: %d consecutive failures (process exited within %v each time). "+
					"Check that php_binary (%q) and laravel_path (%q) are correct and accessible.",
				consecutiveRapidFails, rapidFailWindow, wm.cfg.PHPBinary, wm.cfg.LaravelPath,
			)
			log.Printf("[worker #%d] FAULTED — %s", worker.ID, reason)

			worker.mu.Lock()
			worker.Faulted = true
			worker.FaultReason = reason
			worker.Running = false
			worker.mu.Unlock()
			return
		}

		// ── Exponential backoff ──

		worker.mu.Lock()
		worker.RestartCount++
		restartNum := worker.RestartCount
		worker.mu.Unlock()

		backoff := wm.cfg.RestartDelay * time.Duration(1<<uint(consecutiveRapidFails))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		log.Printf("[worker #%d] Scheduling restart #%d in %v (rapid fails: %d/%d)...",
			worker.ID, restartNum, backoff, consecutiveRapidFails, rapidFailThreshold)

		select {
		case <-time.After(backoff):
			// Continue to restart.
		case <-wm.ctx.Done():
			return
		}
	}
}

// buildArtisanArgs constructs artisan arguments from a worker's individual config.
func (wm *WorkerManager) buildArtisanArgs(wcfg *WorkerConfig) []string {
	args := []string{"artisan", "queue:work"}

	if wcfg.QueueConnection != "" {
		args = append(args, wcfg.QueueConnection)
	}

	if wcfg.QueueNames != "" {
		args = append(args, fmt.Sprintf("--queue=%s", wcfg.QueueNames))
	}

	if wcfg.MaxMemoryMB > 0 {
		args = append(args, fmt.Sprintf("--memory=%d", wcfg.MaxMemoryMB))
	}

	if wcfg.TimeoutSeconds > 0 {
		args = append(args, fmt.Sprintf("--timeout=%d", wcfg.TimeoutSeconds))
	}

	if wcfg.MaxJobs > 0 {
		args = append(args, fmt.Sprintf("--max-jobs=%d", wcfg.MaxJobs))
	}

	tries := wcfg.Tries
	if tries <= 0 {
		tries = 3
	}
	args = append(args, fmt.Sprintf("--tries=%d", tries))

	// Verbose output enables job event logging for history tracking.
	args = append(args, "-v")

	return args
}

// removeWorker removes a worker from the managed slice by ID.
func (wm *WorkerManager) removeWorker(id int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for i, w := range wm.workers {
		if w.ID == id {
			wm.workers = append(wm.workers[:i], wm.workers[i+1:]...)
			return
		}
	}
}
