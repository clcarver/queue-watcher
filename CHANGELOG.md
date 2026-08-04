# Changelog

All notable changes to queue-watcher are documented in this file.

## [0.2.6] - 2026-08-04

### Fixed
- MetricsStore now properly closed on shutdown with WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`) to prevent stat loss during manual updates.
- `StopAll()` now uses `php artisan queue:restart` semantics — waits for running jobs to complete (up to 90s) before force-killing workers.

### Added
- "Stop Service" button on the dashboard sidebar for graceful shutdown without needing PowerShell/sc.exe.
- `POST /api/service/stop` endpoint triggers graceful shutdown (stops workers, checkpoints DB, exits).

## [0.2.5] - 2026-08-04

### Changed
- Companion compatibility check now uses semver range constraints (e.g., `>=1.0.0 <2.0.0`) instead of requiring an exact tag match. Both packages can be versioned independently.

### Added
- New `companion_requires` config field for specifying the acceptable companion version range.

## [0.2.4] - 2026-08-04

### Fixed
- Self-update helper process is now fully detached from the service process tree (uses `CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS`) so the SCM cannot kill it when terminating the service.
- Helper now aborts if the service process does not exit within the timeout instead of blindly proceeding.
- Binary swap uses proper `Move-Item -Force` with error propagation.

### Added
- Internal update log (`update-log.txt`) written by the helper recording: service stop, process exit wait, binary swap, service start, and running verification with timestamps.

## [0.2.3] - 2026-08-03

### Added
- Update compatibility gating with a companion repository tag check (`clcarver/queue-watcher-laravel` by default) before applying queue-watcher updates.
- Dashboard/API support for immediate updates via `POST /api/update/apply` and an **Update Now** button.

### Changed
- Git hot-reload detection now ignores fetch-only ref updates and triggers on local branch updates after pull/merge.
- Auto-updater and CLI manual update now enforce companion tag compatibility before applying updates.

## [0.2.2] - 2026-07-31

### Fixed
- Sidebar layout now stays full-height (`h-full`/viewport height) and no longer scales with main-content scroll length.

## [0.2.1] - 2026-07-31

### Added
- Log viewer backend and API endpoints:
  - `GET /api/site/logs/files`
  - `GET /api/site/logs/entries`
- Log parsing support for Laravel/Monolog files with pagination and filtering.
- SMTP mailer configuration and notification support for:
  - queue job failures
  - error-level application log events
- New dashboard tab layout:
  - **Queue Workers**
  - **Logs**
  - **Mails** (placeholder)
- Failed-job drill-in modal with full exception/stack trace.
- Failed-job actions to retry or delete from the UI.
- Global settings API support for notification configuration.

### Changed
- Hot-reload worker restart behavior now uses `php artisan queue:restart` semantics for graceful restarts before force-stop fallback.
- Job history table pagination defaults to 25 rows per page.
- Failed jobs table pagination defaults to 25 rows per page.
- Dashboard redesigned with a log-viewer-inspired interface and richer controls.

### Fixed
- New-failed-job detection now correctly emits notification callbacks based on the previous high-water mark.
- Log file path handling now uses platform-safe path joining.
- Processed/failed totals are now persisted in durable SQLite counters so deployment/restart cycles do not reset dashboard stats to zero.
