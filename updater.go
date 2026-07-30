package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// UpdateConfig holds auto-update settings.
type UpdateConfig struct {
	Enabled       bool          `json:"enabled"`
	Repository    string        `json:"repository"`     // e.g. "ccarver/queue-watcher"
	CheckInterval time.Duration `json:"check_interval"` // how often to check for updates
}

// DefaultUpdateConfig returns sensible defaults for auto-update.
func DefaultUpdateConfig() *UpdateConfig {
	return &UpdateConfig{
		Enabled:       true,
		Repository:    "clcarver/queue-watcher",
		CheckInterval: 1 * time.Hour,
	}
}

// ghRelease represents a GitHub Release from the API.
type ghRelease struct {
	TagName    string    `json:"tag_name"`
	Name       string    `json:"name"`
	Draft      bool      `json:"draft"`
	Prerelease bool      `json:"prerelease"`
	Published  time.Time `json:"published_at"`
	Assets     []ghAsset `json:"assets"`
}

// ghAsset represents a release asset (downloadable file).
type ghAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

// Updater handles periodic update checks and self-replacement.
type Updater struct {
	config     *UpdateConfig
	httpClient *http.Client
	ctx        context.Context
	cancel     context.CancelFunc

	// Status fields (read by dashboard).
	LastCheck   time.Time `json:"last_check"`
	LastError   string    `json:"last_error,omitempty"`
	Available   string    `json:"available_version,omitempty"`
	UpdateReady bool      `json:"update_ready"`
}

// NewUpdater creates an Updater with the given config.
func NewUpdater(cfg *UpdateConfig) *Updater {
	ctx, cancel := context.WithCancel(context.Background())
	return &Updater{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins the periodic update check loop.
func (u *Updater) Start() {
	if !u.config.Enabled || u.config.Repository == "" {
		log.Println("[updater] Auto-update disabled (no repository configured)")
		return
	}

	log.Printf("[updater] Auto-update enabled, checking %s every %v", u.config.Repository, u.config.CheckInterval)

	go u.loop()
}

// Stop cancels the update loop.
func (u *Updater) Stop() {
	u.cancel()
}

func (u *Updater) loop() {
	// Check immediately on startup (after a short delay to let service settle).
	select {
	case <-time.After(30 * time.Second):
	case <-u.ctx.Done():
		return
	}

	u.check()

	ticker := time.NewTicker(u.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			u.check()
		case <-u.ctx.Done():
			return
		}
	}
}

// check queries GitHub for the latest release and applies if newer.
func (u *Updater) check() {
	u.LastCheck = time.Now()
	u.LastError = ""

	release, err := u.fetchLatestRelease()
	if err != nil {
		u.LastError = err.Error()
		log.Printf("[updater] Failed to check for updates: %v", err)
		return
	}

	if release.Draft || release.Prerelease {
		return
	}

	latestVersion := normalizeVersion(release.TagName)
	currentVersion := normalizeVersion(version)

	if latestVersion == currentVersion || latestVersion == "dev" {
		log.Printf("[updater] Up to date (%s)", version)
		return
	}

	if !isNewer(latestVersion, currentVersion) {
		return
	}

	u.Available = release.TagName
	log.Printf("[updater] New version available: %s (current: %s)", release.TagName, version)

	// Find the right asset for this OS/arch.
	asset := u.findAsset(release)
	if asset == nil {
		u.LastError = "no compatible binary in release assets"
		log.Printf("[updater] No compatible asset found for %s/%s", runtime.GOOS, runtime.GOARCH)
		return
	}

	// Download and apply.
	if err := u.downloadAndApply(asset); err != nil {
		u.LastError = err.Error()
		log.Printf("[updater] Failed to apply update: %v", err)
		return
	}

	u.UpdateReady = true
	log.Printf("[updater] Update applied successfully. Restarting...")

	// Restart the process. For a Windows Service, we exit and the SCM restarts us.
	// For interactive mode, we also just exit.
	go func() {
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()
}

// fetchLatestRelease gets the latest release from GitHub API.
func (u *Updater) fetchLatestRelease() (*ghRelease, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", u.config.Repository)

	req, err := http.NewRequestWithContext(u.ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "queue-watcher/"+version)

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("repository or release not found")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GitHub API returned status %d", resp.StatusCode)
	}

	var release ghRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("failed to parse release JSON: %w", err)
	}

	return &release, nil
}

// findAsset locates the appropriate binary asset for the current OS/architecture.
func (u *Updater) findAsset(release *ghRelease) *ghAsset {
	// Expected naming: queue-watcher-windows-amd64.exe, queue-watcher-linux-amd64, etc.
	wantOS := runtime.GOOS
	wantArch := runtime.GOARCH

	for i := range release.Assets {
		name := strings.ToLower(release.Assets[i].Name)
		if strings.Contains(name, wantOS) && strings.Contains(name, wantArch) {
			return &release.Assets[i]
		}
	}

	// Fallback: look for just .exe on Windows.
	if wantOS == "windows" {
		for i := range release.Assets {
			if strings.HasSuffix(strings.ToLower(release.Assets[i].Name), ".exe") {
				return &release.Assets[i]
			}
		}
	}

	return nil
}

// downloadAndApply downloads the asset and replaces the current binary.
func (u *Updater) downloadAndApply(asset *ghAsset) error {
	log.Printf("[updater] Downloading %s (%d bytes)...", asset.Name, asset.Size)

	req, err := http.NewRequestWithContext(u.ctx, "GET", asset.BrowserDownloadURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "queue-watcher/"+version)

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("download returned status %d", resp.StatusCode)
	}

	// Get the path to the current executable.
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot determine executable path: %w", err)
	}
	exePath, err = filepath.EvalSymlinks(exePath)
	if err != nil {
		return fmt.Errorf("cannot resolve executable path: %w", err)
	}

	// Write to a temp file in the same directory (ensures same filesystem for rename).
	dir := filepath.Dir(exePath)
	tmpFile, err := os.CreateTemp(dir, "queue-watcher-update-*.tmp")
	if err != nil {
		return fmt.Errorf("cannot create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	_, err = io.Copy(tmpFile, resp.Body)
	tmpFile.Close()
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("download write failed: %w", err)
	}

	// On Windows, we cannot overwrite a running executable directly.
	// Strategy: rename current → .old, rename new → current.
	oldPath := exePath + ".old"

	// Remove any previous .old file.
	os.Remove(oldPath)

	// Rename running exe → .old
	if err := os.Rename(exePath, oldPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("cannot rename current binary: %w", err)
	}

	// Rename downloaded temp → exe path.
	if err := os.Rename(tmpPath, exePath); err != nil {
		// Try to restore the old binary.
		os.Rename(oldPath, exePath)
		os.Remove(tmpPath)
		return fmt.Errorf("cannot place new binary: %w", err)
	}

	log.Printf("[updater] Binary replaced successfully at %s", exePath)
	return nil
}

// CheckNow performs an immediate update check (used by CLI/API).
func (u *Updater) CheckNow() (*ghRelease, error) {
	return u.fetchLatestRelease()
}

// normalizeVersion strips the "v" prefix and returns the semver string.
func normalizeVersion(v string) string {
	return strings.TrimPrefix(strings.TrimSpace(v), "v")
}

// isNewer returns true if candidate is a newer semver than current.
// Simple comparison: splits on "." and compares numerically.
func isNewer(candidate, current string) bool {
	if current == "dev" || current == "unknown" {
		return true // Always update from dev builds.
	}

	cParts := strings.Split(candidate, ".")
	oParts := strings.Split(current, ".")

	for i := 0; i < 3; i++ {
		var c, o int
		if i < len(cParts) {
			fmt.Sscanf(cParts[i], "%d", &c)
		}
		if i < len(oParts) {
			fmt.Sscanf(oParts[i], "%d", &o)
		}
		if c > o {
			return true
		}
		if c < o {
			return false
		}
	}
	return false
}

// Status returns the current updater status for the dashboard.
func (u *Updater) Status() map[string]interface{} {
	return map[string]interface{}{
		"enabled":           u.config.Enabled,
		"repository":        u.config.Repository,
		"current_version":   version,
		"available_version": u.Available,
		"last_check":        u.LastCheck,
		"last_error":        u.LastError,
		"update_ready":      u.UpdateReady,
	}
}
