package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// GitWatcher monitors the Laravel project for code deployments (git pull).
// It uses periodic polling of Git reference files to detect changes.
type GitWatcher struct {
	cfg     *Config
	ctx     context.Context
	manager *WorkerManager

	mu             sync.RWMutex
	lastDeployment time.Time
	lastHash       string
	deployCount    int
}

// NewGitWatcher creates a new Git deployment watcher.
func NewGitWatcher(cfg *Config, ctx context.Context, manager *WorkerManager) *GitWatcher {
	return &GitWatcher{
		cfg:     cfg,
		ctx:     ctx,
		manager: manager,
	}
}

// Run starts the periodic Git polling loop.
func (gw *GitWatcher) Run() {
	log.Printf("[watcher] Monitoring Git changes in: %s (branch: %s, interval: %v)",
		gw.cfg.LaravelPath, gw.cfg.GitBranch, gw.cfg.GitPollInterval)

	// Capture initial state so we don't trigger on startup.
	gw.lastHash = gw.computeHash()

	ticker := time.NewTicker(gw.cfg.GitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gw.ctx.Done():
			log.Println("[watcher] Git watcher shutting down.")
			return

		case <-ticker.C:
			gw.checkForChanges()
		}
	}
}

// TriggerReload forces a manual hot-reload regardless of Git changes.
func (gw *GitWatcher) TriggerReload() {
	log.Println("[watcher] Manual hot-reload triggered.")

	gw.mu.Lock()
	gw.lastDeployment = time.Now()
	gw.deployCount++
	gw.mu.Unlock()

	// Restart all workers with fresh code.
	gw.manager.RestartAll()

	// Update the hash baseline to avoid re-triggering.
	gw.mu.Lock()
	gw.lastHash = gw.computeHash()
	gw.mu.Unlock()
}

// GetLastDeployment returns the timestamp of the last detected deployment.
func (gw *GitWatcher) GetLastDeployment() time.Time {
	gw.mu.RLock()
	defer gw.mu.RUnlock()
	return gw.lastDeployment
}

// GetDeployCount returns total number of deployments detected.
func (gw *GitWatcher) GetDeployCount() int {
	gw.mu.RLock()
	defer gw.mu.RUnlock()
	return gw.deployCount
}

// checkForChanges compares the current Git state hash against the last known hash.
func (gw *GitWatcher) checkForChanges() {
	currentHash := gw.computeHash()

	gw.mu.RLock()
	lastHash := gw.lastHash
	gw.mu.RUnlock()

	if currentHash != lastHash && currentHash != "" {
		log.Println("[watcher] Git change detected! Initiating hot-reload...")

		gw.mu.Lock()
		gw.lastHash = currentHash
		gw.lastDeployment = time.Now()
		gw.deployCount++
		gw.mu.Unlock()

		// Perform graceful restart of all workers.
		gw.manager.RestartAll()
	}
}

// computeHash generates a combined hash of Git reference files to detect changes.
// We monitor multiple files that change on git pull/fetch/merge.
func (gw *GitWatcher) computeHash() string {
	hasher := sha256.New()

	// Files that change when code is updated via git pull.
	watchFiles := []string{
		filepath.Join(gw.cfg.LaravelPath, ".git", "refs", "heads", gw.cfg.GitBranch),
		filepath.Join(gw.cfg.LaravelPath, ".git", "FETCH_HEAD"),
		filepath.Join(gw.cfg.LaravelPath, ".git", "HEAD"),
		filepath.Join(gw.cfg.LaravelPath, ".git", "ORIG_HEAD"),
	}

	for _, path := range watchFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		hasher.Write(data)

		// Also include modification time for extra sensitivity.
		info, err := os.Stat(path)
		if err == nil {
			hasher.Write([]byte(info.ModTime().String()))
		}
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}
