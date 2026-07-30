package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Site represents a single Laravel project with its own worker pool and git watcher.
type Site struct {
	Config    *SiteConfig
	Manager   *WorkerManager
	Watcher   *GitWatcher
	DB        *LaravelDB    // Direct connection to the Laravel app's database.
	Metrics   *MetricsStore // Local SQLite store for job history retention.
	Running   bool
	mu        sync.Mutex
}

// SiteManager manages multiple Laravel sites, each with independent workers.
type SiteManager struct {
	cfg   *Config
	sites map[string]*Site
	mu    sync.RWMutex
	ctx   context.Context
}

// NewSiteManager creates a new multi-site manager.
func NewSiteManager(cfg *Config, ctx context.Context) *SiteManager {
	sm := &SiteManager{
		cfg:   cfg,
		sites: make(map[string]*Site),
		ctx:   ctx,
	}

	// Initialize sites from config.
	for _, sc := range cfg.Sites {
		sm.startSite(sc)
	}

	return sm
}

// GetSite returns a site by ID (thread-safe).
func (sm *SiteManager) GetSite(id string) *Site {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sites[id]
}

// GetAllSites returns a snapshot of all sites (thread-safe).
func (sm *SiteManager) GetAllSites() []*Site {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sites := make([]*Site, 0, len(sm.sites))
	for _, s := range sm.sites {
		sites = append(sites, s)
	}
	return sites
}

// AddSite validates a directory, registers a new site, starts its workers, and persists to config.
func (sm *SiteManager) AddSite(sc *SiteConfig) error {
	// Validate the Laravel installation.
	if err := ValidateLaravelProject(sc.LaravelPath); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Generate a unique ID if not set.
	if sc.ID == "" {
		sc.ID = GenerateSiteID(sc.LaravelPath)
	}
	sc.ID = sm.cfg.EnsureUniqueSiteID(sc.ID)

	// Apply defaults for any zero-value fields.
	if sc.GitBranch == "" {
		sc.GitBranch = "main"
	}
	if sc.Name == "" {
		sc.Name = filepath.Base(sc.LaravelPath)
	}

	// Migrate or initialize worker configs.
	sc.MigrateWorkers()
	if len(sc.Workers) == 0 {
		sc.Workers = []*WorkerConfig{DefaultWorkerConfig()}
	}

	// Register in config and persist.
	sm.cfg.AddSite(sc)
	if err := sm.cfg.SaveDefault(); err != nil {
		log.Printf("[sites] Warning: failed to persist config: %v", err)
	}

	// Start the site.
	sm.startSite(sc)

	log.Printf("[sites] Site %q (%s) added and started with %d workers.", sc.Name, sc.ID, len(sc.Workers))
	return nil
}

// UpdateSite applies new settings to an existing site. It stops all current
// workers and respawns them with the updated configuration.
func (sm *SiteManager) UpdateSite(id string, updates *SiteConfig) error {
	sm.mu.Lock()
	site, exists := sm.sites[id]
	if !exists {
		sm.mu.Unlock()
		return fmt.Errorf("site %q not found", id)
	}
	sm.mu.Unlock()

	// Apply non-zero updates to the existing config.
	cfg := site.Config
	if updates.Name != "" {
		cfg.Name = updates.Name
	}
	if updates.GitBranch != "" {
		cfg.GitBranch = updates.GitBranch
	}

	// Always apply DB env key mappings (empty string clears the mapping).
	cfg.DBHostEnv = updates.DBHostEnv
	cfg.DBPortEnv = updates.DBPortEnv
	cfg.DBDatabaseEnv = updates.DBDatabaseEnv
	cfg.DBUsernameEnv = updates.DBUsernameEnv
	cfg.DBPasswordEnv = updates.DBPasswordEnv

	// Stop the old workers and watcher.
	site.Manager.StopAll()

	// Remove old site entry.
	sm.mu.Lock()
	delete(sm.sites, id)
	sm.mu.Unlock()

	// Update the persisted config.
	for _, s := range sm.cfg.Sites {
		if s.ID == id {
			*s = *cfg
			break
		}
	}
	if err := sm.cfg.SaveDefault(); err != nil {
		log.Printf("[sites] Warning: failed to persist config: %v", err)
	}

	// Restart with new settings.
	sm.startSite(cfg)

	log.Printf("[sites] Site %q updated and restarted with %d workers.",
		cfg.Name, len(cfg.Workers))
	return nil
}

// RemoveSite stops all workers for a site and removes it from management.
func (sm *SiteManager) RemoveSite(id string) error {
	sm.mu.Lock()
	site, exists := sm.sites[id]
	if !exists {
		sm.mu.Unlock()
		return fmt.Errorf("site %q not found", id)
	}
	delete(sm.sites, id)
	sm.mu.Unlock()

	// Stop the site's workers.
	site.Manager.StopAll()
	log.Printf("[sites] Site %q removed and workers stopped.", id)

	// Remove from config and persist.
	sm.cfg.RemoveSite(id)
	if err := sm.cfg.SaveDefault(); err != nil {
		log.Printf("[sites] Warning: failed to persist config: %v", err)
	}

	return nil
}

// StopAll gracefully stops all sites.
func (sm *SiteManager) StopAll() {
	sm.mu.RLock()
	sites := make([]*Site, 0, len(sm.sites))
	for _, s := range sm.sites {
		sites = append(sites, s)
	}
	sm.mu.RUnlock()

	for _, s := range sites {
		s.Manager.StopAll()
	}
}

// startSite creates and starts a site's WorkerManager and GitWatcher.
func (sm *SiteManager) startSite(sc *SiteConfig) {
	siteCfg := sm.cfg.ConfigForSite(sc)

	manager := NewWorkerManager(siteCfg, sm.ctx)
	watcher := NewGitWatcher(siteCfg, sm.ctx, manager)

	// Initialize the local SQLite metrics store.
	dataDir := filepath.Join(filepath.Dir(configPath()), "data")
	metricsStore, err := NewMetricsStore(dataDir)
	if err != nil {
		log.Printf("[sites] Warning: failed to open metrics store for %s: %v", sc.ID, err)
	}

	// Connect to the Laravel app's database for live queue state.
	var laravelDB *LaravelDB
	ldb, err := NewLaravelDB(sc)
	if err != nil {
		log.Printf("[sites] Warning: could not connect to Laravel DB for %s: %v", sc.ID, err)
	} else {
		laravelDB = ldb
	}

	site := &Site{
		Config:  sc,
		Manager: manager,
		Watcher: watcher,
		DB:      laravelDB,
		Metrics: metricsStore,
		Running: true,
	}

	// Wire job events from stdout telemetry to the metrics store.
	// This is the PRIMARY source of job lifecycle data — real-time, zero delay.
	if metricsStore != nil {
		manager.OnJobEvent = func(event JobEvent) {
			metricsStore.RecordJobEvent(sc.ID, event)
			if event.Status == "processed" || event.Status == "failed" {
				log.Printf("[stdout] Job %s: %s (worker #%d) exec=%dms",
					event.Status, event.JobName, event.WorkerID, event.DurationMs)
			}
		}
	}

	// DB-diff completions serve as a BACKUP for jobs processed by external workers.
	if laravelDB != nil && metricsStore != nil {
		laravelDB.OnCompletion = func(event JobEvent) {
			metricsStore.RecordJobEvent(sc.ID, event)
		}
	}

	sm.mu.Lock()
	sm.sites[sc.ID] = site
	sm.mu.Unlock()

	// Launch manager and watcher in background goroutines.
	go manager.Run()
	go watcher.Run()

	// Start DB poller if connected.
	if laravelDB != nil {
		go sm.pollLaravelDB(sc.ID, site)
	}

	// Spawn individual workers from config.
	for _, wcfg := range sc.Workers {
		wc := *wcfg // Copy to avoid shared pointer issues.
		manager.SpawnWorkerWithConfig(&wc)
	}
}

// pollLaravelDB periodically reads queue state from the Laravel database.
func (sm *SiteManager) pollLaravelDB(siteID string, site *Site) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	snapshotInterval := 30 * time.Second
	lastSnapshot := time.Time{}

	// Do an immediate first poll.
	if _, err := site.DB.PollMetrics(); err != nil {
		log.Printf("[sites] Initial DB poll failed for %s: %v", siteID, err)
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			metrics, err := site.DB.PollMetrics()
			if err != nil {
				log.Printf("[sites] DB poll error for %s: %v", siteID, err)
				continue
			}

			// Record throughput snapshot at a slower interval for historical retention.
			if site.Metrics != nil && time.Since(lastSnapshot) >= snapshotInterval {
				stats := site.Manager.GetJobHistory().GetStats()
				site.Metrics.RecordThroughputSnapshot(
					siteID,
					metrics.PendingCount,
					metrics.ReservedCount,
					stats.TotalProcessed,
					stats.TotalFailed,
				)
				lastSnapshot = time.Now()
			}
		}
	}
}

// ── Laravel Validation ──

// ValidationResult describes what was found when inspecting a directory.
type ValidationResult struct {
	Valid           bool     `json:"valid"`
	Path            string   `json:"path"`
	IsDirectory     bool     `json:"is_directory"`
	HasArtisan      bool     `json:"has_artisan"`
	HasComposerJSON bool     `json:"has_composer_json"`
	IsLaravel       bool     `json:"is_laravel"`
	HasGit          bool     `json:"has_git"`
	LaravelVersion  string   `json:"laravel_version,omitempty"`
	Errors          []string `json:"errors,omitempty"`
}

// ValidateLaravelProject checks that a directory contains a valid Laravel installation.
func ValidateLaravelProject(dirPath string) error {
	result := InspectLaravelProject(dirPath)
	if !result.Valid {
		return fmt.Errorf("%s", strings.Join(result.Errors, "; "))
	}
	return nil
}

// InspectLaravelProject performs a detailed inspection of a directory.
func InspectLaravelProject(dirPath string) ValidationResult {
	result := ValidationResult{Path: dirPath}

	// Check directory exists.
	info, err := os.Stat(dirPath)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("path does not exist: %v", err))
		return result
	}
	if !info.IsDir() {
		result.Errors = append(result.Errors, "path is not a directory")
		return result
	}
	result.IsDirectory = true

	// Check for artisan file.
	artisanPath := filepath.Join(dirPath, "artisan")
	if _, err := os.Stat(artisanPath); err == nil {
		result.HasArtisan = true
	} else {
		result.Errors = append(result.Errors, "'artisan' file not found — not a Laravel project root")
	}

	// Check for composer.json and laravel/framework dependency.
	composerPath := filepath.Join(dirPath, "composer.json")
	composerData, err := os.ReadFile(composerPath)
	if err == nil {
		result.HasComposerJSON = true

		var composer struct {
			Require map[string]string `json:"require"`
		}
		if err := json.Unmarshal(composerData, &composer); err == nil {
			if version, ok := composer.Require["laravel/framework"]; ok {
				result.IsLaravel = true
				result.LaravelVersion = version
			} else {
				result.Errors = append(result.Errors, "'laravel/framework' not found in composer.json require section")
			}
		}
	} else {
		result.Errors = append(result.Errors, "'composer.json' not found")
	}

	// Check for .git directory (optional but noted).
	gitDir := filepath.Join(dirPath, ".git")
	if info, err := os.Stat(gitDir); err == nil && info.IsDir() {
		result.HasGit = true
	}

	// Valid if we have artisan + laravel/framework.
	result.Valid = result.HasArtisan && result.IsLaravel

	return result
}
