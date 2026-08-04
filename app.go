package main

import (
	"context"
	"log"
	"os"
	"sync"
)

// Application orchestrates all subsystems: site manager, dashboard, and updater.
type Application struct {
	cfg     *Config
	sites   *SiteManager
	server  *DashboardServer
	updater *Updater
	mailer  *Mailer
	cancel  context.CancelFunc
	ctx     context.Context
	wg      sync.WaitGroup
}

// NewApplication constructs a fully wired Application instance.
func NewApplication(cfg *Config) *Application {
	ctx, cancel := context.WithCancel(context.Background())

	// Build mailer first so SiteManager can reference it.
	var mailer *Mailer
	if cfg.Notify != nil {
		mailer = NewMailer(cfg.Notify)
	}

	sites := NewSiteManager(cfg, ctx, mailer)
	server := NewDashboardServer(cfg, sites)

	var updater *Updater
	if cfg.Update != nil {
		updater = NewUpdater(cfg.Update)
		server.updater = updater
	}

	app := &Application{
		cfg:     cfg,
		sites:   sites,
		server:  server,
		updater: updater,
		mailer:  mailer,
		cancel:  cancel,
		ctx:     ctx,
	}

	// Wire the dashboard's shutdown button to a graceful stop.
	server.OnShutdown = func() {
		log.Println("[app] Shutdown requested via dashboard.")
		app.Stop()
		os.Exit(0)
	}

	return app
}

// Start launches the dashboard HTTP server and auto-updater.
func (a *Application) Start() {
	log.Printf("[app] Starting with %d registered sites, dashboard on %s",
		len(a.cfg.Sites), a.cfg.DashboardAddr)

	// Start dashboard HTTP server.
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.server.Run(a.ctx)
	}()

	// Start auto-updater.
	if a.updater != nil {
		a.updater.Start()
	}
}

// Stop gracefully shuts down all subsystems and waits for completion.
func (a *Application) Stop() {
	log.Println("[app] Initiating graceful shutdown...")

	// Stop auto-updater.
	if a.updater != nil {
		a.updater.Stop()
	}

	// Signal all goroutines to stop.
	a.cancel()

	// Stop all site workers.
	a.sites.StopAll()

	// Wait for all goroutines to finish.
	a.wg.Wait()

	log.Println("[app] All subsystems stopped.")
}

