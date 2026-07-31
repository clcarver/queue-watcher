package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// WorkerConfig holds per-worker artisan command settings.
type WorkerConfig struct {
	Label          string `json:"label"`           // Human label, e.g. "Mail Worker"
	QueueConnection string `json:"queue_connection"` // e.g. "redis", "database"
	QueueNames     string `json:"queue_names"`      // e.g. "default", "mail,notifications"
	MaxMemoryMB    int    `json:"max_memory_mb"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	MaxJobs        int    `json:"max_jobs"`
	Tries          int    `json:"tries"`
}

// SiteConfig holds per-site configuration for a single Laravel project.
type SiteConfig struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	LaravelPath string          `json:"laravel_path"`
	GitBranch   string          `json:"git_branch"`
	Workers     []*WorkerConfig `json:"workers"` // Per-worker configs (replaces WorkerCount).

	// Database connection: names of keys in the site's .env file.
	// Example: "db_host_env": "PROD_SQL_DB_HOST" → reads PROD_SQL_DB_HOST from .env.
	DBHostEnv     string `json:"db_host_env,omitempty"`
	DBPortEnv     string `json:"db_port_env,omitempty"`
	DBDatabaseEnv string `json:"db_database_env,omitempty"`
	DBUsernameEnv string `json:"db_username_env,omitempty"`
	DBPasswordEnv string `json:"db_password_env,omitempty"`

	// Legacy field — used only for migration from old configs.
	WorkerCount     int    `json:"worker_count,omitempty"`
	QueueConnection string `json:"queue_connection,omitempty"`
	QueueNames      string `json:"queue_names,omitempty"`
	MaxMemoryMB     int    `json:"max_memory_mb,omitempty"`
	TimeoutSeconds  int    `json:"timeout_seconds,omitempty"`
	MaxJobs         int    `json:"max_jobs,omitempty"`
}

// MigrateWorkers converts legacy WorkerCount-based configs to per-worker configs.
// Called once at load time for backward compatibility.
func (sc *SiteConfig) MigrateWorkers() {
	if len(sc.Workers) > 0 {
		return // Already has per-worker configs.
	}
	count := sc.WorkerCount
	if count < 1 {
		count = 2
	}
	for i := 0; i < count; i++ {
		sc.Workers = append(sc.Workers, &WorkerConfig{
			Label:           fmt.Sprintf("Worker %d", i+1),
			QueueConnection: sc.QueueConnection,
			QueueNames:      sc.QueueNames,
			MaxMemoryMB:     sc.MaxMemoryMB,
			TimeoutSeconds:  sc.TimeoutSeconds,
			MaxJobs:         sc.MaxJobs,
			Tries:           3,
		})
	}
}

// DefaultWorkerConfig returns a worker config with production defaults.
func DefaultWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		Label:           "Default Worker",
		QueueConnection: "redis",
		QueueNames:      "default",
		MaxMemoryMB:     128,
		TimeoutSeconds:  60,
		MaxJobs:         0,
		Tries:           3,
	}
}

// Config holds global application configuration and registered sites.
type Config struct {
	// Path to the PHP executable (global, shared by all sites).
	PHPBinary string `json:"php_binary"`

	// Throttle delay before restarting a crashed worker.
	RestartDelay time.Duration `json:"restart_delay"`

	// Interval for polling Git changes.
	GitPollInterval time.Duration `json:"git_poll_interval"`

	// HTTP dashboard listen address.
	DashboardAddr string `json:"dashboard_addr"`

	// Auto-update settings.
	Update *UpdateConfig `json:"update,omitempty"`

	// Email notification settings.
	Notify *NotifyConfig `json:"notify,omitempty"`

	// Maximum number of job_events rows to retain per site (auto-prune).
	// Older rows are deleted once this limit is exceeded. 0 = disabled.
	MaxRetainedRecords int `json:"max_retained_records"`

	// Registered Laravel sites.
	Sites []*SiteConfig `json:"sites"`

	// ── Per-site overrides (not serialized, populated by ConfigForSite) ──

	LaravelPath string `json:"-"`
	WorkerCount int    `json:"-"`
	GitBranch   string `json:"-"`
}

// DefaultConfig returns a configuration with sensible production defaults.
func DefaultConfig() *Config {
	return &Config{
		PHPBinary:       `php`,
		RestartDelay:    2 * time.Second,
		GitPollInterval: 10 * time.Second,
		DashboardAddr:   "127.0.0.1:9100",
		Update:          DefaultUpdateConfig(),
		Notify:          DefaultNotifyConfig(),
		MaxRetainedRecords: 10000,
		Sites:           []*SiteConfig{},
	}
}

// DefaultSiteConfig returns default per-site settings.
func DefaultSiteConfig() *SiteConfig {
	return &SiteConfig{
		GitBranch: "main",
		Workers:   []*WorkerConfig{DefaultWorkerConfig()},
	}
}

// ConfigForSite creates a flat Config compatible with WorkerManager/GitWatcher
// by merging global settings with a specific site's settings.
func (c *Config) ConfigForSite(site *SiteConfig) *Config {
	return &Config{
		PHPBinary:       c.PHPBinary,
		RestartDelay:    c.RestartDelay,
		GitPollInterval: c.GitPollInterval,
		DashboardAddr:   c.DashboardAddr,
		// Per-site overrides used by WorkerManager/GitWatcher.
		LaravelPath:     site.LaravelPath,
		GitBranch:       site.GitBranch,
		WorkerCount:     len(site.Workers),
	}
}

// FindSite returns a site config by ID, or nil if not found.
func (c *Config) FindSite(id string) *SiteConfig {
	for _, s := range c.Sites {
		if s.ID == id {
			return s
		}
	}
	return nil
}

// AddSite appends a site to the config.
func (c *Config) AddSite(site *SiteConfig) {
	c.Sites = append(c.Sites, site)
}

// RemoveSite removes a site by ID. Returns true if found and removed.
func (c *Config) RemoveSite(id string) bool {
	for i, s := range c.Sites {
		if s.ID == id {
			c.Sites = append(c.Sites[:i], c.Sites[i+1:]...)
			return true
		}
	}
	return false
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

// GenerateSiteID creates a URL-safe slug from a directory path.
func GenerateSiteID(dirPath string) string {
	base := filepath.Base(dirPath)
	slug := strings.ToLower(base)
	slug = slugRe.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "site"
	}
	return slug
}

// EnsureUniqueSiteID appends a suffix if the ID already exists.
func (c *Config) EnsureUniqueSiteID(id string) string {
	if c.FindSite(id) == nil {
		return id
	}
	for i := 2; i < 1000; i++ {
		candidate := fmt.Sprintf("%s-%d", id, i)
		if c.FindSite(candidate) == nil {
			return candidate
		}
	}
	return fmt.Sprintf("%s-%d", id, time.Now().UnixNano())
}

// configPath returns the JSON config file path adjacent to the executable.
func configPath() string {
	exePath, err := os.Executable()
	if err != nil {
		return "queue-watcher.json"
	}
	return filepath.Join(filepath.Dir(exePath), "queue-watcher.json")
}

// LoadConfig reads configuration from a JSON file adjacent to the executable.
// If the file does not exist, it creates one with default values.
func LoadConfig() (*Config, error) {
	path := configPath()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			cfg := DefaultConfig()
			_ = cfg.SaveDefault()
			return cfg, nil
		}
		return DefaultConfig(), err
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return DefaultConfig(), err
	}

	// Enforce minimum restart delay to prevent CPU spin.
	if cfg.RestartDelay < 1*time.Second {
		cfg.RestartDelay = 2 * time.Second
	}

	// Ensure update config exists (may be nil if JSON has "update": null).
	if cfg.Update == nil {
		cfg.Update = DefaultUpdateConfig()
	}

	// Enforce minimums on all sites and migrate legacy configs.
	for _, s := range cfg.Sites {
		s.MigrateWorkers()
	}

	return cfg, nil
}

// SaveDefault writes the config to its default location.
func (c *Config) SaveDefault() error {
	return c.Save(configPath())
}

// Save writes the config to disk as formatted JSON.
func (c *Config) Save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
