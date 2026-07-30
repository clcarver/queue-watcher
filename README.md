# Queue Watcher

A Windows service that supervises Laravel queue workers with auto-restart, Git-based hot-reload, real-time job telemetry, and a web dashboard.

## Features

- **Worker Supervision** — Manages multiple `php artisan queue:work` processes with exponential backoff and circuit breaker
- **Git Hot-Reload** — Detects deployments and gracefully restarts workers
- **Real-Time Job Tracking** — Parses structured JSON telemetry from stdout for instant job visibility
- **Database Integration** — Reads MSSQL `jobs`/`failed_jobs` tables for complete queue state
- **Per-Queue Metrics** — Average wait time, execution time, throughput per queue
- **Web Dashboard** — Live stats, worker management, job history at a glance
- **Auto-Update** — Checks GitHub Releases and self-updates with zero downtime
- **Multi-Site** — Monitor multiple Laravel applications from one instance

## Quick Start

### 1. Download

Grab the latest `queue-watcher-windows-amd64.exe` from [Releases](https://github.com/clcarver/queue-watcher/releases).

### 2. Configure

Copy `queue-watcher.example.json` → `queue-watcher.json` next to the exe:

```json
{
  "dashboard_addr": "0.0.0.0:9100",
  "update": {
    "enabled": true,
    "repository": "clcarver/queue-watcher",
    "check_interval": 3600000000000
  },
  "sites": [
    {
      "id": "my-app",
      "name": "My Laravel App",
      "laravel_path": "C:\\path\\to\\laravel",
      "git_branch": "main",
      "workers": [
        {
          "label": "Default Worker",
          "queue_connection": "database",
          "queue_names": "default",
          "max_memory_mb": 128,
          "timeout_seconds": 60,
          "tries": 3
        }
      ],
      "db_host_env": "DB_HOST",
      "db_port_env": "DB_PORT",
      "db_database_env": "DB_DATABASE",
      "db_username_env": "DB_USERNAME",
      "db_password_env": "DB_PASSWORD"
    }
  ]
}
```

The `db_*_env` fields are **key names** from your Laravel `.env` file. The app reads the `.env` and uses those keys to connect to MSSQL for live queue metrics.

### 3. Install as Windows Service

```powershell
# As Administrator
sc.exe create LaravelQueueWatcher binPath= "C:\path\to\queue-watcher.exe" start= auto
sc.exe description LaravelQueueWatcher "Supervises Laravel queue workers"
sc.exe failure LaravelQueueWatcher reset= 60 actions= restart/5000/restart/10000/restart/30000
sc.exe start LaravelQueueWatcher
```

The `failure` line ensures the service auto-restarts after self-updates.

### 4. Access Dashboard

Open `http://localhost:9100` in your browser.

## Auto-Update

When `update.enabled` is `true` in config, the service:

1. Checks GitHub Releases every `check_interval` (default: 1 hour)
2. Compares the latest release tag against its built-in version
3. Downloads the new binary if newer
4. Replaces itself (rename trick for Windows) and exits
5. Windows SCM auto-restarts the service with the new version

### Manual Update

```powershell
.\queue-watcher.exe update
```

## Development

### Build

```powershell
go build -ldflags "-X main.version=v0.1.0 -X main.commit=$(git rev-parse --short HEAD) -X main.buildDate=$(Get-Date -Format yyyy-MM-dd)" -o queue-watcher.exe .
```

### Run Interactively

```powershell
.\queue-watcher.exe run
```

### Release

Tag and push — GitHub Actions builds and publishes automatically:

```powershell
git tag v1.0.0
git push origin v1.0.0
```

## Laravel Integration

For real-time job telemetry (zero-latency), add this to your `AppServiceProvider::boot()`:

```php
if ($this->app->runningInConsole()) {
    Queue::before(function (JobProcessing $event) {
        $this->logToJson('reserved', $event->job);
    });
    Queue::after(function (JobProcessed $event) {
        $this->logToJson('completed', $event->job);
    });
    Queue::failing(function (JobFailed $event) {
        $this->logToJson('failed', $event->job, $event->exception->getMessage());
    });
}
```

With the private method:

```php
private function logToJson(string $status, $job, ?string $error = null): void
{
    echo json_encode([
        'telemetry' => true,
        'status' => $status,
        'job_id' => $job->getJobId(),
        'job_name' => $job->resolveName(),
        'queue' => $job->getQueue(),
        'error' => $error,
        'time' => now()->toISOString(),
    ]) . PHP_EOL;
}
```

Queue Watcher parses these JSON lines from stdout in real-time — no database polling delay.

## Architecture

```
┌─────────────────────────────────────────────┐
│           Queue Watcher (Go binary)          │
├─────────────┬───────────────┬───────────────┤
│  Supervisor │   Git Watcher │   Dashboard   │
│  (workers)  │   (hot-reload)│   (HTTP/API)  │
├─────────────┴───────────────┴───────────────┤
│            Auto-Updater (GitHub)             │
├─────────────────────────────────────────────┤
│  stdout JSON telemetry  │  MSSQL polling    │
│  (primary, instant)     │  (backup, 2s)     │
├─────────────────────────┴───────────────────┤
│          SQLite (local retention)            │
└─────────────────────────────────────────────┘
```
