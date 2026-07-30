package main

import (
	"log"

	"golang.org/x/sys/windows/svc"
)

// queueWatcherService implements the svc.Handler interface for Windows Service lifecycle.
type queueWatcherService struct{}

// Execute is the main service loop called by the Windows Service Control Manager.
// It handles Start, Stop, Shutdown, and Interrogate commands.
func (s *queueWatcherService) Execute(args []string, changeReq <-chan svc.ChangeRequest, statusChan chan<- svc.Status) (bool, uint32) {
	// Define which control commands we accept.
	const acceptedCmds = svc.AcceptStop | svc.AcceptShutdown

	// Notify SCM that we are starting.
	statusChan <- svc.Status{State: svc.StartPending}

	log.Println("[service] Loading configuration...")
	cfg, err := LoadConfig()
	if err != nil {
		log.Printf("[service] Warning: config load issue: %v (using defaults)", err)
	}

	// Initialize and start the core application.
	app := NewApplication(cfg)
	app.Start()

	// Notify SCM that we are now running.
	statusChan <- svc.Status{State: svc.Running, Accepts: acceptedCmds}
	log.Println("[service] Service is running.")

	// Main service event loop.
	for {
		select {
		case req := <-changeReq:
			switch req.Cmd {
			case svc.Interrogate:
				// Respond with current status.
				statusChan <- req.CurrentStatus

			case svc.Stop, svc.Shutdown:
				log.Println("[service] Received stop/shutdown signal. Gracefully terminating...")
				statusChan <- svc.Status{State: svc.StopPending}

				// Gracefully shut down all components.
				app.Stop()

				log.Println("[service] Shutdown complete.")
				return false, 0

			default:
				log.Printf("[service] Unexpected control request: %v", req.Cmd)
			}
		}
	}
}
