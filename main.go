// Package main implements a Windows Service that supervises Laravel queue workers.
// It provides automatic process management, Git deployment detection, and a web dashboard.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/sys/windows/svc"
)

const serviceName = "LaravelQueueWatcher"

func main() {
	// Detect if we are running as a Windows Service or interactively.
	inService, err := svc.IsWindowsService()
	if err != nil {
		log.Fatalf("failed to determine service context: %v", err)
	}

	if inService {
		// Running as a Windows Service — hand off to the service control manager.
		if err := svc.Run(serviceName, &queueWatcherService{}); err != nil {
			log.Fatalf("service execution failed: %v", err)
		}
		return
	}

	// Interactive mode — support basic CLI commands for development/testing.
	if len(os.Args) > 1 {
		cmd := strings.ToLower(os.Args[1])
		switch cmd {
		case "run":
			// Run in foreground (useful for debugging).
			runInteractive()
		case "version":
			fmt.Println(VersionString())
		case "update":
			// Manual update check.
			runUpdate()
		default:
			printUsage()
		}
		return
	}

	printUsage()
}

func printUsage() {
	fmt.Printf(`%s

Usage:
  queue-watcher.exe run       Run interactively in the foreground (for debugging)
  queue-watcher.exe version   Print version information
  queue-watcher.exe update    Check for and apply updates immediately

When installed as a Windows Service, the binary runs automatically
and checks for updates periodically (configurable in queue-watcher.json).

Installation (PowerShell as Administrator):
  sc.exe create LaravelQueueWatcher binPath= "C:\path\to\queue-watcher.exe" start= auto
  sc.exe description LaravelQueueWatcher "Supervises Laravel queue workers with auto-restart and deployment monitoring"
  sc.exe failure LaravelQueueWatcher reset= 60 actions= restart/5000/restart/10000/restart/30000
  sc.exe start LaravelQueueWatcher

The "failure" line ensures the service restarts automatically after updates.
`, VersionString())
}

// runInteractive starts the application in foreground mode for development.
func runInteractive() {
	log.Printf("[queue-watcher] %s", VersionString())
	log.Println("[queue-watcher] Starting in interactive (foreground) mode...")

	cfg, err := LoadConfig()
	if err != nil {
		log.Printf("[queue-watcher] Warning: config load issue: %v (using defaults)", err)
	}

	app := NewApplication(cfg)
	app.Start()

	log.Println("[queue-watcher] Running. Press Ctrl+C to stop.")

	// Block until interrupted.
	select {}
}

// runUpdate performs a one-shot update check from the command line.
func runUpdate() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if cfg.Update == nil || cfg.Update.Repository == "" {
		fmt.Println("Auto-update not configured. Set 'update.repository' in queue-watcher.json.")
		fmt.Println(`Example: "update": {"enabled": true, "repository": "yourorg/queue-watcher", "check_interval": 3600000000000}`)
		os.Exit(1)
	}

	updater := NewUpdater(cfg.Update)

	fmt.Printf("Current version: %s\n", version)
	fmt.Printf("Checking %s for updates...\n", cfg.Update.Repository)

	release, err := updater.CheckNow()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	latestVersion := normalizeVersion(release.TagName)
	currentVersion := normalizeVersion(version)

	if !isNewer(latestVersion, currentVersion) {
		fmt.Printf("Already up to date (%s)\n", version)
		return
	}

	fmt.Printf("New version available: %s\n", release.TagName)

	asset := updater.findAsset(release)
	if asset == nil {
		fmt.Println("Error: no compatible binary found in release assets")
		os.Exit(1)
	}

	fmt.Printf("Downloading %s...\n", asset.Name)
	if err := updater.downloadAndApply(asset); err != nil {
		fmt.Printf("Update failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Update applied successfully! Restart the service to use the new version.")
}

