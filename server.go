package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//go:embed templates/*
var embeddedFS embed.FS

// DashboardServer provides a web-based control panel for the queue watcher.
type DashboardServer struct {
	cfg     *Config
	sites   *SiteManager
	updater *Updater
	tmpl    *template.Template
	server  *http.Server
}

// NewDashboardServer creates a new dashboard HTTP server.
func NewDashboardServer(cfg *Config, sites *SiteManager) *DashboardServer {
	tmpl, err := template.ParseFS(embeddedFS, "templates/*.html")
	if err != nil {
		log.Fatalf("[dashboard] Failed to parse embedded templates: %v", err)
	}

	return &DashboardServer{
		cfg:   cfg,
		sites: sites,
		tmpl:  tmpl,
	}
}

// Run starts the HTTP server. It blocks until the context is cancelled.
func (ds *DashboardServer) Run(ctx context.Context) {
	mux := http.NewServeMux()

	// Dashboard page.
	mux.HandleFunc("/", ds.handleDashboard)

	// Site management APIs.
	mux.HandleFunc("/api/sites", ds.handleAPISites)
	mux.HandleFunc("/api/sites/add", ds.handleAPISiteAdd)
	mux.HandleFunc("/api/sites/edit", ds.handleAPISiteEdit)
	mux.HandleFunc("/api/sites/remove", ds.handleAPISiteRemove)
	mux.HandleFunc("/api/sites/validate", ds.handleAPISiteValidate)
	mux.HandleFunc("/api/sites/env-keys", ds.handleAPISiteEnvKeys)

	// Per-site worker APIs.
	mux.HandleFunc("/api/site/status", ds.handleAPISiteStatus)
	mux.HandleFunc("/api/site/spawn", ds.handleAPISiteSpawn)
	mux.HandleFunc("/api/site/stop", ds.handleAPISiteStop)
	mux.HandleFunc("/api/site/delete", ds.handleAPISiteDelete)
	mux.HandleFunc("/api/site/reload", ds.handleAPISiteReload)
	mux.HandleFunc("/api/site/jobs", ds.handleAPISiteJobs)
	mux.HandleFunc("/api/site/queue", ds.handleAPISiteQueue)
	mux.HandleFunc("/api/site/worker/edit", ds.handleAPIWorkerEdit)
	mux.HandleFunc("/api/site/failed-job/detail", ds.handleAPIFailedJobDetail)
	mux.HandleFunc("/api/site/failed-job/delete", ds.handleAPIFailedJobDelete)
	mux.HandleFunc("/api/site/failed-job/retry", ds.handleAPIFailedJobRetry)

	// Log viewer APIs.
	mux.HandleFunc("/api/site/logs/files", ds.handleAPILogFiles)
	mux.HandleFunc("/api/site/logs/entries", ds.handleAPILogEntries)

	// Updater API.
	mux.HandleFunc("/api/update/status", ds.handleAPIUpdateStatus)
	mux.HandleFunc("/api/update/check", ds.handleAPIUpdateCheck)

	// Settings API.
	mux.HandleFunc("/api/settings", ds.handleAPISettings)

	ds.server = &http.Server{
		Addr:         ds.cfg.DashboardAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("[dashboard] HTTP server starting on http://%s", ds.cfg.DashboardAddr)

	go func() {
		if err := ds.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[dashboard] HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ds.server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[dashboard] HTTP server shutdown error: %v", err)
	} else {
		log.Println("[dashboard] HTTP server stopped gracefully.")
	}
}

// ── Dashboard Page ──

// DashboardData holds template data for the initial page render.
type DashboardData struct {
	DashboardAddr string `json:"dashboard_addr"`
}

func (ds *DashboardServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data := DashboardData{
		DashboardAddr: ds.cfg.DashboardAddr,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := ds.tmpl.ExecuteTemplate(w, "dashboard.html", data); err != nil {
		log.Printf("[dashboard] Template render error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// ── Site Management APIs ──

// SiteStatusData is the JSON payload for a single site's status.
type SiteStatusData struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	LaravelPath     string         `json:"laravel_path"`
	GitBranch       string         `json:"git_branch"`
	WorkerCount     int            `json:"worker_count"`
	Workers         []WorkerStatus `json:"workers"`
	LastDeployment  string         `json:"last_deployment"`
	DeployCount     int            `json:"deploy_count"`
	JobStats        JobStats       `json:"job_stats"`
	QueueConnection string         `json:"queue_connection"`
	QueueNames      string         `json:"queue_names"`
	MaxMemoryMB     int            `json:"max_memory_mb"`
	TimeoutSeconds  int            `json:"timeout_seconds"`
	MaxJobs         int            `json:"max_jobs"`
	// Live queue metrics from the Laravel database.
	QueueMetrics    *QueueMetrics  `json:"queue_metrics,omitempty"`
	DBConnected     bool           `json:"db_connected"`
	DBError         string         `json:"db_error,omitempty"`
	// Configured .env key mappings for DB connection.
	DBHostEnv       string         `json:"db_host_env,omitempty"`
	DBPortEnv       string         `json:"db_port_env,omitempty"`
	DBDatabaseEnv   string         `json:"db_database_env,omitempty"`
	DBUsernameEnv   string         `json:"db_username_env,omitempty"`
	DBPasswordEnv   string         `json:"db_password_env,omitempty"`
}

// handleAPISites returns JSON list of all sites with their worker statuses.
func (ds *DashboardServer) handleAPISites(w http.ResponseWriter, r *http.Request) {
	sites := ds.sites.GetAllSites()

	result := make([]SiteStatusData, 0, len(sites))
	for _, s := range sites {
		result = append(result, ds.buildSiteStatus(s))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleAPISiteAdd registers a new Laravel site.
func (ds *DashboardServer) handleAPISiteAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Path            string `json:"path"`
		Name            string `json:"name"`
		WorkerCount     int    `json:"worker_count"`
		QueueConnection string `json:"queue_connection"`
		QueueNames      string `json:"queue_names"`
		GitBranch       string `json:"git_branch"`
		DBHostEnv       string `json:"db_host_env"`
		DBPortEnv       string `json:"db_port_env"`
		DBDatabaseEnv   string `json:"db_database_env"`
		DBUsernameEnv   string `json:"db_username_env"`
		DBPasswordEnv   string `json:"db_password_env"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Path == "" {
		jsonError(w, "path is required", http.StatusBadRequest)
		return
	}

	sc := &SiteConfig{
		Name:            req.Name,
		LaravelPath:     req.Path,
		WorkerCount:     req.WorkerCount,
		QueueConnection: req.QueueConnection,
		QueueNames:      req.QueueNames,
		GitBranch:       req.GitBranch,
		DBHostEnv:       req.DBHostEnv,
		DBPortEnv:       req.DBPortEnv,
		DBDatabaseEnv:   req.DBDatabaseEnv,
		DBUsernameEnv:   req.DBUsernameEnv,
		DBPasswordEnv:   req.DBPasswordEnv,
	}

	if err := ds.sites.AddSite(sc); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"site_id": sc.ID,
		"message": fmt.Sprintf("Site %q added with %d workers.", sc.Name, sc.WorkerCount),
	})
}

// handleAPISiteEdit updates an existing site's name/branch and restarts workers.
// Per-worker settings are edited via /api/site/worker/edit.
func (ds *DashboardServer) handleAPISiteEdit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		GitBranch     string `json:"git_branch"`
		DBHostEnv     string `json:"db_host_env"`
		DBPortEnv     string `json:"db_port_env"`
		DBDatabaseEnv string `json:"db_database_env"`
		DBUsernameEnv string `json:"db_username_env"`
		DBPasswordEnv string `json:"db_password_env"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		jsonError(w, "id is required", http.StatusBadRequest)
		return
	}

	updates := &SiteConfig{
		Name:          req.Name,
		GitBranch:     req.GitBranch,
		DBHostEnv:     req.DBHostEnv,
		DBPortEnv:     req.DBPortEnv,
		DBDatabaseEnv: req.DBDatabaseEnv,
		DBUsernameEnv: req.DBUsernameEnv,
		DBPasswordEnv: req.DBPasswordEnv,
	}

	if err := ds.sites.UpdateSite(req.ID, updates); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Site %q updated.", req.ID),
	})
}

// handleAPISiteRemove removes a site and stops its workers.
func (ds *DashboardServer) handleAPISiteRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	siteID := r.URL.Query().Get("id")
	if siteID == "" {
		jsonError(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	if err := ds.sites.RemoveSite(siteID); err != nil {
		jsonError(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Site %q removed.", siteID),
	})
}

// handleAPISiteValidate validates a directory without adding it.
func (ds *DashboardServer) handleAPISiteValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	result := InspectLaravelProject(req.Path)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleAPISiteEnvKeys reads the .env file from a Laravel project directory
// and returns all available key names so the frontend can populate dropdowns.
func (ds *DashboardServer) handleAPISiteEnvKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Path == "" {
		jsonError(w, "path is required", http.StatusBadRequest)
		return
	}

	env, err := ReadLaravelEnv(req.Path)
	if err != nil {
		jsonError(w, fmt.Sprintf("Failed to read .env: %v", err), http.StatusBadRequest)
		return
	}

	// Return all key names sorted, plus the current site's configured mappings if editing.
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"keys": keys,
	})
}

// ── Per-Site Worker APIs ──

// handleAPISiteStatus returns status for a specific site.
func (ds *DashboardServer) handleAPISiteStatus(w http.ResponseWriter, r *http.Request) {
	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ds.buildSiteStatus(site))
}

// handleAPISiteSpawn spawns a new worker for a specific site with individual config.
func (ds *DashboardServer) handleAPISiteSpawn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	var req WorkerConfig
	if r.Body != nil && r.ContentLength > 0 {
		json.NewDecoder(r.Body).Decode(&req)
	}

	// Use defaults if nothing provided.
	if req.QueueNames == "" {
		req.QueueNames = "default"
	}

	wcfg := &req
	id := site.Manager.SpawnWorkerWithConfig(wcfg)

	// Persist the new worker to config.
	site.Config.Workers = append(site.Config.Workers, wcfg)
	_ = ds.cfg.SaveDefault()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Worker #%d spawned (%s).", id, wcfg.QueueNames),
	})
}

// handleAPIWorkerEdit updates a specific worker's config and restarts it.
func (ds *DashboardServer) handleAPIWorkerEdit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	var req struct {
		WorkerID       int    `json:"worker_id"`
		Label          string `json:"label"`
		QueueConnection string `json:"queue_connection"`
		QueueNames     string `json:"queue_names"`
		MaxMemoryMB    int    `json:"max_memory_mb"`
		TimeoutSeconds int    `json:"timeout_seconds"`
		MaxJobs        int    `json:"max_jobs"`
		Tries          int    `json:"tries"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	newCfg := &WorkerConfig{
		Label:           req.Label,
		QueueConnection: req.QueueConnection,
		QueueNames:      req.QueueNames,
		MaxMemoryMB:     req.MaxMemoryMB,
		TimeoutSeconds:  req.TimeoutSeconds,
		MaxJobs:         req.MaxJobs,
		Tries:           req.Tries,
	}

	if !site.Manager.EditWorker(req.WorkerID, newCfg) {
		jsonError(w, fmt.Sprintf("Worker #%d not found", req.WorkerID), http.StatusNotFound)
		return
	}

	// Persist: rebuild the site's worker config list from running workers.
	statuses := site.Manager.GetStatuses()
	site.Config.Workers = make([]*WorkerConfig, 0, len(statuses))
	for _, s := range statuses {
		site.Config.Workers = append(site.Config.Workers, &WorkerConfig{
			Label:           s.Label,
			QueueConnection: s.QueueConnection,
			QueueNames:      s.QueueNames,
			MaxMemoryMB:     s.MaxMemoryMB,
			TimeoutSeconds:  s.TimeoutSeconds,
			MaxJobs:         s.MaxJobs,
			Tries:           s.Tries,
		})
	}
	_ = ds.cfg.SaveDefault()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Worker updated and restarted with queue=%s.", req.QueueNames),
	})
}

// handleAPISiteStop stops a worker for a specific site.
func (ds *DashboardServer) handleAPISiteStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	workerID, err := strconv.Atoi(r.URL.Query().Get("worker_id"))
	if err != nil {
		jsonError(w, "Invalid 'worker_id' parameter", http.StatusBadRequest)
		return
	}

	success := site.Manager.StopWorker(workerID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": success,
		"message": fmt.Sprintf("Worker #%d stop request processed.", workerID),
	})
}

// handleAPISiteDelete deletes a worker from a specific site.
func (ds *DashboardServer) handleAPISiteDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	workerID, err := strconv.Atoi(r.URL.Query().Get("worker_id"))
	if err != nil {
		jsonError(w, "Invalid 'worker_id' parameter", http.StatusBadRequest)
		return
	}

	success := site.Manager.DeleteWorker(workerID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": success,
		"message": fmt.Sprintf("Worker #%d deleted.", workerID),
	})
}

// handleAPISiteReload triggers a hot-reload for a specific site.
func (ds *DashboardServer) handleAPISiteReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	go site.Watcher.TriggerReload()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Hot-reload initiated for site %q.", site.Config.Name),
	})
}

// handleAPISiteJobs returns paginated job history for a specific site.
func (ds *DashboardServer) handleAPISiteJobs(w http.ResponseWriter, r *http.Request) {
	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	// Default to 25 per page; allow override via ?limit=N (max 500).
	limit := 25
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 500 {
		limit = n
	}

	// Page offset via ?page=N (1-based).
	page := 1
	if n, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && n > 1 {
		page = n
	}
	offset := (page - 1) * limit

	// Prefer SQLite-backed history if available; fall back to in-memory ring buffer.
	var events []JobEvent
	var totalCount int
	var stats JobStats
	var queueStats []QueueStat
	if site.Metrics != nil {
		events, totalCount = site.Metrics.GetPagedEvents(site.Config.ID, limit, offset)
		stats = site.Metrics.GetStats(site.Config.ID)
		queueStats = site.Metrics.GetQueueStats(site.Config.ID)
	} else {
		all := site.Manager.GetJobHistory().Recent(limit + offset)
		if offset < len(all) {
			events = all[offset:]
		}
		totalCount = len(all)
		stats = site.Manager.GetJobHistory().GetStats()
	}

	// Include in-flight count from the live DB metrics.
	if site.DB != nil {
		cached := site.DB.GetCachedMetrics()
		stats.InFlight = cached.ReservedCount
	}

	totalPages := (totalCount + limit - 1) / limit
	if totalPages < 1 {
		totalPages = 1
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"events":      events,
		"stats":       stats,
		"queue_stats": queueStats,
		"pagination": map[string]interface{}{
			"page":        page,
			"limit":       limit,
			"total":       totalCount,
			"total_pages": totalPages,
		},
	})
}

// handleAPISiteQueue returns live queue metrics from the Laravel database.
func (ds *DashboardServer) handleAPISiteQueue(w http.ResponseWriter, r *http.Request) {
	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	result := map[string]interface{}{
		"db_connected": false,
	}

	if site.DB != nil {
		result["db_connected"] = site.DB.IsConnected()
		result["db_error"] = site.DB.GetLastError()
		result["metrics"] = site.DB.GetCachedMetrics()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ── Helpers ──

// requireSite extracts and validates the site_id query parameter.
// Returns nil and writes an error response if the site is not found.
func (ds *DashboardServer) requireSite(w http.ResponseWriter, r *http.Request) *Site {
	siteID := r.URL.Query().Get("site_id")
	if siteID == "" {
		jsonError(w, "Missing 'site_id' parameter", http.StatusBadRequest)
		return nil
	}

	site := ds.sites.GetSite(siteID)
	if site == nil {
		jsonError(w, fmt.Sprintf("Site %q not found", siteID), http.StatusNotFound)
		return nil
	}

	return site
}

// buildSiteStatus assembles status data for a single site.
func (ds *DashboardServer) buildSiteStatus(s *Site) SiteStatusData {
	lastDeploy := s.Watcher.GetLastDeployment()
	deployStr := "Never"
	if !lastDeploy.IsZero() {
		deployStr = lastDeploy.Format("2006-01-02 15:04:05 MST")
	}

	// Use SQLite stats if available for persistence across restarts.
	var jobStats JobStats
	if s.Metrics != nil {
		jobStats = s.Metrics.GetStats(s.Config.ID)
	} else {
		jobStats = s.Manager.GetJobHistory().GetStats()
	}

	// In-flight comes from the live DB (reserved count).
	if s.DB != nil {
		cached := s.DB.GetCachedMetrics()
		jobStats.InFlight = cached.ReservedCount
	}

	status := SiteStatusData{
		ID:              s.Config.ID,
		Name:            s.Config.Name,
		LaravelPath:     s.Config.LaravelPath,
		GitBranch:       s.Config.GitBranch,
		WorkerCount:     s.Manager.GetWorkerCount(),
		Workers:         s.Manager.GetStatuses(),
		LastDeployment:  deployStr,
		DeployCount:     s.Watcher.GetDeployCount(),
		JobStats:        jobStats,
		QueueConnection: s.Config.QueueConnection,
		QueueNames:      s.Config.QueueNames,
		MaxMemoryMB:     s.Config.MaxMemoryMB,
		TimeoutSeconds:  s.Config.TimeoutSeconds,
		MaxJobs:         s.Config.MaxJobs,
	}

	// Attach live queue metrics from the Laravel DB if connected.
	if s.DB != nil {
		status.DBConnected = s.DB.IsConnected()
		status.DBError = s.DB.GetLastError()
		status.QueueMetrics = s.DB.GetCachedMetrics()
	}

	// Include the configured .env key mappings.
	status.DBHostEnv = s.Config.DBHostEnv
	status.DBPortEnv = s.Config.DBPortEnv
	status.DBDatabaseEnv = s.Config.DBDatabaseEnv
	status.DBUsernameEnv = s.Config.DBUsernameEnv
	status.DBPasswordEnv = s.Config.DBPasswordEnv

	return status
}

// jsonError writes a JSON error response.
func jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": message,
	})
}

// handleAPIUpdateStatus returns the current auto-updater status.
func (ds *DashboardServer) handleAPIUpdateStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if ds.updater == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"enabled":         false,
			"current_version": version,
		})
		return
	}

	json.NewEncoder(w).Encode(ds.updater.Status())
}

// handleAPIUpdateCheck triggers an immediate check for a newer release.
func (ds *DashboardServer) handleAPIUpdateCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if ds.updater == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"current_version": version,
			"up_to_date":      true,
			"message":         "Auto-updater not configured.",
		})
		return
	}

	release, err := ds.updater.CheckNow()
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"current_version": version,
			"error":           err.Error(),
		})
		return
	}

	latestVersion := normalizeVersion(release.TagName)
	currentVersion := normalizeVersion(version)
	upToDate := !isNewer(latestVersion, currentVersion)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"current_version": version,
		"latest_version":  release.TagName,
		"up_to_date":      upToDate,
		"published_at":    release.Published,
	})
}

// handleAPILogFiles lists available log files for a site.
func (ds *DashboardServer) handleAPILogFiles(w http.ResponseWriter, r *http.Request) {
	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	files, err := ListLogFiles(site.Config.LaravelPath)
	if err != nil {
		jsonError(w, "Cannot list log files: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"files": files,
	})
}

// handleAPILogEntries returns paginated, filtered log entries from a log file.
func (ds *DashboardServer) handleAPILogEntries(w http.ResponseWriter, r *http.Request) {
	site := ds.requireSite(w, r)
	if site == nil {
		return
	}

	fileName := r.URL.Query().Get("file")
	if fileName == "" {
		jsonError(w, "Missing 'file' parameter", http.StatusBadRequest)
		return
	}

	// Sanitize: strip any path separators to prevent directory traversal.
	for _, c := range []string{"/", "\\", ".."} {
		if strings.Contains(fileName, c) {
			jsonError(w, "Invalid file name", http.StatusBadRequest)
			return
		}
	}

	filePath := filepath.Join(site.Config.LaravelPath, "storage", "logs", fileName)

	limit := 50
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 500 {
		limit = n
	}
	page := 1
	if n, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && n > 1 {
		page = n
	}

	filterLevel := r.URL.Query().Get("level")
	search := r.URL.Query().Get("search")

	result, err := ReadLogEntries(filePath, page, limit, filterLevel, search)
	if err != nil {
		jsonError(w, "Cannot read log file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleAPISettings returns or updates global settings (including notify config).
func (ds *DashboardServer) handleAPISettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	notify := ds.cfg.Notify
	if notify == nil {
		notify = DefaultNotifyConfig()
	}

	if r.Method == http.MethodGet {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"max_retained_records": ds.cfg.MaxRetainedRecords,
			"notify":               notify,
		})
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		MaxRetainedRecords int           `json:"max_retained_records"`
		Notify             *NotifyConfig `json:"notify,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.MaxRetainedRecords >= 0 {
		ds.cfg.MaxRetainedRecords = req.MaxRetainedRecords
	}
	if req.Notify != nil {
		ds.cfg.Notify = req.Notify
	}

	if err := ds.cfg.SaveDefault(); err != nil {
		jsonError(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":              true,
		"max_retained_records": ds.cfg.MaxRetainedRecords,
	})
}

// handleAPIFailedJobDetail returns the full exception for a failed job.
func (ds *DashboardServer) handleAPIFailedJobDetail(w http.ResponseWriter, r *http.Request) {
	site := ds.requireSite(w, r)
	if site == nil {
		return
	}
	if site.DB == nil {
		jsonError(w, "No database connection", http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		jsonError(w, "Invalid 'id' parameter", http.StatusBadRequest)
		return
	}

	job, err := site.DB.GetFailedJobDetail(id)
	if err != nil {
		jsonError(w, "Failed job not found: "+err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// handleAPIFailedJobDelete deletes a failed job by UUID.
func (ds *DashboardServer) handleAPIFailedJobDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}
	if site.DB == nil {
		jsonError(w, "No database connection", http.StatusBadRequest)
		return
	}

	var req struct {
		UUID string `json:"uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UUID == "" {
		jsonError(w, "uuid is required", http.StatusBadRequest)
		return
	}

	if err := site.DB.DeleteFailedJob(req.UUID); err != nil {
		jsonError(w, "Delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "message": "Failed job deleted."})
}

// handleAPIFailedJobRetry re-queues a failed job by UUID.
func (ds *DashboardServer) handleAPIFailedJobRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	site := ds.requireSite(w, r)
	if site == nil {
		return
	}
	if site.DB == nil {
		jsonError(w, "No database connection", http.StatusBadRequest)
		return
	}

	var req struct {
		UUID string `json:"uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UUID == "" {
		jsonError(w, "uuid is required", http.StatusBadRequest)
		return
	}

	if err := site.DB.RetryFailedJob(req.UUID); err != nil {
		jsonError(w, "Retry failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "message": "Job pushed back onto the queue."})
}
