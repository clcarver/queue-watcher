package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/windows/svc"
)

// UpdateConfig holds auto-update settings.
type UpdateConfig struct {
	Enabled             bool          `json:"enabled"`
	Repository          string        `json:"repository"`                     // e.g. "ccarver/queue-watcher"
	CheckInterval       time.Duration `json:"check_interval"`                 // how often to check for updates
	CompanionRepository string        `json:"companion_repository,omitempty"` // e.g. "ccarver/queue-watcher-laravel"
}

// DefaultUpdateConfig returns sensible defaults for auto-update.
func DefaultUpdateConfig() *UpdateConfig {
	return &UpdateConfig{
		Enabled:             true,
		Repository:          "clcarver/queue-watcher",
		CheckInterval:       1 * time.Hour,
		CompanionRepository: "clcarver/queue-watcher-laravel",
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

// ghTag represents a Git tag from the GitHub API.
type ghTag struct {
	Name string `json:"name"`
}

// UpdatePreview describes update availability and compatibility.
type UpdatePreview struct {
	CurrentVersion      string
	LatestVersion       string
	PublishedAt         time.Time
	UpToDate            bool
	CanUpdate           bool
	CompanionRepository string
	CompanionTag        string
	CompatibilityReason string
}

// UpdateApplyResult describes the outcome of a forced update attempt.
type UpdateApplyResult struct {
	Updated      bool
	UpdateReady  bool
	Current      string
	Latest       string
	CompanionTag string
	Message      string
}

// Updater handles periodic update checks and self-replacement.
type Updater struct {
	config     *UpdateConfig
	httpClient *http.Client
	ctx        context.Context
	cancel     context.CancelFunc

	// Status fields (read by dashboard).
	LastCheck    time.Time `json:"last_check"`
	LastError    string    `json:"last_error,omitempty"`
	Available    string    `json:"available_version,omitempty"`
	UpdateReady  bool      `json:"update_ready"`
	CompanionTag string    `json:"companion_tag,omitempty"`
	CompatOK     bool      `json:"compat_ok"`
	CompatReason string    `json:"compat_reason,omitempty"`
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

	preview, err := u.PreviewUpdate()
	if err != nil {
		u.LastError = err.Error()
		log.Printf("[updater] Failed to check for updates: %v", err)
		return
	}

	if preview.UpToDate {
		u.Available = ""
		u.CompanionTag = preview.CompanionTag
		u.CompatOK = true
		u.CompatReason = ""
		log.Printf("[updater] Up to date (%s)", version)
		return
	}
	u.Available = preview.LatestVersion
	u.CompanionTag = preview.CompanionTag
	u.CompatOK = preview.CanUpdate
	u.CompatReason = preview.CompatibilityReason
	if !preview.CanUpdate {
		log.Printf("[updater] Update available but blocked: %s", preview.CompatibilityReason)
		return
	}

	// Find the right asset for this OS/arch.
	release, err := u.fetchLatestRelease()
	if err != nil {
		u.LastError = err.Error()
		log.Printf("[updater] Failed to re-fetch release: %v", err)
		return
	}
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
	if isWindowsServiceProcess() {
		log.Printf("[updater] Update staged. Helper will gracefully stop, update, and restart service %q.", serviceName)
		return
	}

	log.Printf("[updater] Update applied successfully. Restarting process...")
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

// fetchLatestTag gets the latest tag for a repository.
func (u *Updater) fetchLatestTag(repository string) (string, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/tags?per_page=1", repository)

	req, err := http.NewRequestWithContext(u.ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "queue-watcher/"+version)

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return "", fmt.Errorf("repository %q not found", repository)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("GitHub API returned status %d for %s", resp.StatusCode, repository)
	}

	var tags []ghTag
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return "", fmt.Errorf("failed to parse tags JSON: %w", err)
	}
	if len(tags) == 0 {
		return "", fmt.Errorf("repository %q has no tags", repository)
	}
	return tags[0].Name, nil
}

// PreviewUpdate checks for an update and validates companion repository compatibility.
func (u *Updater) PreviewUpdate() (*UpdatePreview, error) {
	release, err := u.fetchLatestRelease()
	if err != nil {
		return nil, err
	}

	if release.Draft || release.Prerelease {
		return &UpdatePreview{
			CurrentVersion: version,
			LatestVersion:  release.TagName,
			PublishedAt:    release.Published,
			UpToDate:       true,
		}, nil
	}

	latestVersion := normalizeVersion(release.TagName)
	currentVersion := normalizeVersion(version)

	preview := &UpdatePreview{
		CurrentVersion:      version,
		LatestVersion:       release.TagName,
		PublishedAt:         release.Published,
		CompanionRepository: u.config.CompanionRepository,
	}

	if latestVersion == currentVersion || latestVersion == "dev" || !isNewer(latestVersion, currentVersion) {
		preview.UpToDate = true
		preview.CanUpdate = false
		return preview, nil
	}

	preview.CanUpdate = true
	if u.config.CompanionRepository == "" {
		return preview, nil
	}

	tag, err := u.fetchLatestTag(u.config.CompanionRepository)
	if err != nil {
		return nil, fmt.Errorf("companion check failed: %w", err)
	}
	preview.CompanionTag = tag

	if normalizeVersion(tag) != normalizeVersion(release.TagName) {
		preview.CanUpdate = false
		preview.CompatibilityReason = fmt.Sprintf(
			"blocked: queue-watcher %s requires %s tag %s, latest tag is %s",
			release.TagName,
			u.config.CompanionRepository,
			release.TagName,
			tag,
		)
	}

	return preview, nil
}

// ApplyUpdateNow performs an immediate update attempt.
func (u *Updater) ApplyUpdateNow() (*UpdateApplyResult, error) {
	preview, err := u.PreviewUpdate()
	if err != nil {
		return nil, err
	}

	result := &UpdateApplyResult{
		UpdateReady:  u.UpdateReady,
		Current:      preview.CurrentVersion,
		Latest:       preview.LatestVersion,
		CompanionTag: preview.CompanionTag,
	}

	if preview.UpToDate {
		result.Message = "Already up to date."
		return result, nil
	}
	if !preview.CanUpdate {
		result.Message = preview.CompatibilityReason
		return result, nil
	}

	release, err := u.fetchLatestRelease()
	if err != nil {
		return nil, err
	}
	asset := u.findAsset(release)
	if asset == nil {
		return nil, fmt.Errorf("no compatible binary in release assets")
	}

	if err := u.downloadAndApply(asset); err != nil {
		return nil, err
	}

	u.LastCheck = time.Now()
	u.LastError = ""
	u.Available = release.TagName
	u.CompanionTag = preview.CompanionTag
	u.CompatOK = true
	u.CompatReason = ""
	u.UpdateReady = true

	result.Updated = true
	result.UpdateReady = true
	if isWindowsServiceProcess() {
		result.Message = "Update staged. Service restart is being handled automatically."
	} else {
		result.Message = "Update applied. Restart queue-watcher to run the new version."
	}

	return result, nil
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

	// On Windows Service, stage update and delegate replacement/restart to a helper
	// process that stops the service gracefully, swaps binaries after exit, then starts it.
	if runtime.GOOS == "windows" && isWindowsServiceProcess() {
		if err := u.stageServiceUpdate(exePath, tmpPath); err != nil {
			os.Remove(tmpPath)
			return err
		}
		return nil
	}

	// Default path (interactive/dev): replace binary in place.
	// On Windows this may require process exit immediately after this function returns.
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

// stageServiceUpdate launches an external PowerShell helper that:
// 1) stops the Windows service gracefully
// 2) waits for this process to fully exit
// 3) swaps binaries
// 4) restarts the service
func (u *Updater) stageServiceUpdate(exePath, tmpPath string) error {
	oldPath := exePath + ".old"
	currentPID := os.Getpid()

	script := fmt.Sprintf(`
$ErrorActionPreference = 'Stop'
$service = '%s'
$pidToWait = %d
$exe = '%s'
$tmp = '%s'
$old = '%s'

sc.exe stop $service | Out-Null

for ($i = 0; $i -lt 240; $i++) {
    Start-Sleep -Milliseconds 500
    if (-not (Get-Process -Id $pidToWait -ErrorAction SilentlyContinue)) { break }
}

for ($i = 0; $i -lt 40; $i++) {
    try {
        Remove-Item -Force $old -ErrorAction SilentlyContinue
        Move-Item -Force $exe $old
        Move-Item -Force $tmp $exe
        break
    } catch {
        if ($i -ge 39) { throw }
        Start-Sleep -Milliseconds 500
    }
}

sc.exe start $service | Out-Null
`, escapeSingle(serviceName), currentPID, escapeSingle(exePath), escapeSingle(tmpPath), escapeSingle(oldPath))

	cmd := exec.Command(
		"powershell",
		"-NoProfile",
		"-NonInteractive",
		"-ExecutionPolicy", "Bypass",
		"-Command", script,
	)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to launch service update helper: %w", err)
	}

	log.Printf("[updater] Service update helper started (pid=%d).", cmd.Process.Pid)
	return nil
}

func isWindowsServiceProcess() bool {
	if runtime.GOOS != "windows" {
		return false
	}
	inService, err := svc.IsWindowsService()
	return err == nil && inService
}

func escapeSingle(s string) string {
	return strings.ReplaceAll(s, "'", "''")
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
		"enabled":              u.config.Enabled,
		"repository":           u.config.Repository,
		"companion_repository": u.config.CompanionRepository,
		"current_version":      version,
		"available_version":    u.Available,
		"last_check":           u.LastCheck,
		"last_error":           u.LastError,
		"update_ready":         u.UpdateReady,
		"companion_tag":        u.CompanionTag,
		"compat_ok":            u.CompatOK,
		"compat_reason":        u.CompatReason,
	}
}
