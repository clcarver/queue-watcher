package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"strings"
	"time"
)

// NotifyConfig holds email notification settings.
type NotifyConfig struct {
	Enabled    bool     `json:"enabled"`
	SMTPHost   string   `json:"smtp_host"`
	SMTPPort   int      `json:"smtp_port"`
	Username   string   `json:"username"`
	Password   string   `json:"password"`
	FromAddr   string   `json:"from_addr"`
	FromName   string   `json:"from_name"`
	Recipients []string `json:"recipients"`

	// What to notify on.
	OnJobFailed  bool `json:"on_job_failed"`
	OnLogError   bool `json:"on_log_error"`
}

// DefaultNotifyConfig returns sensible defaults.
func DefaultNotifyConfig() *NotifyConfig {
	return &NotifyConfig{
		Enabled:     false,
		SMTPPort:    587,
		OnJobFailed: true,
		OnLogError:  true,
	}
}

// Mailer sends email notifications.
type Mailer struct {
	cfg *NotifyConfig
}

// NewMailer creates a Mailer from config.
func NewMailer(cfg *NotifyConfig) *Mailer {
	return &Mailer{cfg: cfg}
}

// SendJobFailedAlert sends an alert email when a queue job fails.
func (m *Mailer) SendJobFailedAlert(siteName string, job *FailedJob) {
	if m == nil || m.cfg == nil || !m.cfg.Enabled || !m.cfg.OnJobFailed {
		return
	}
	if len(m.cfg.Recipients) == 0 {
		return
	}

	subject := fmt.Sprintf("[Queue Watcher] Job Failed — %s on %s", job.JobName, siteName)

	body := fmt.Sprintf(`Queue Job Failure Alert
=======================
Site:       %s
Job:        %s
Queue:      %s
UUID:       %s
Failed At:  %s
Connection: %s

Exception:
----------
%s

-- Sent by Queue Watcher at %s
`, siteName, job.JobName, job.Queue, job.UUID, job.FailedAt, job.Connection,
		job.Exception, time.Now().Format("2006-01-02 15:04:05"))

	go m.send(subject, body)
}

// SendLogErrorAlert sends an alert email when an error-level log entry is detected.
func (m *Mailer) SendLogErrorAlert(siteName, logFile string, entry LogEntry) {
	if m == nil || m.cfg == nil || !m.cfg.Enabled || !m.cfg.OnLogError {
		return
	}
	if len(m.cfg.Recipients) == 0 {
		return
	}

	subject := fmt.Sprintf("[Queue Watcher] Log %s — %s on %s",
		string(entry.Level), truncate(entry.Message, 80), siteName)

	body := fmt.Sprintf(`Application Log Error Alert
===========================
Site:      %s
Log File:  %s
Level:     %s
Channel:   %s
Timestamp: %s

Message:
--------
%s

Context:
--------
%s

%s
-- Sent by Queue Watcher at %s
`, siteName, logFile, string(entry.Level), entry.Channel,
		entry.Timestamp.Format("2006-01-02 15:04:05"),
		entry.Message, entry.Context, entry.Extra,
		time.Now().Format("2006-01-02 15:04:05"))

	go m.send(subject, body)
}

// send delivers a plain-text email via SMTP.
func (m *Mailer) send(subject, body string) {
	cfg := m.cfg
	if cfg.SMTPHost == "" || len(cfg.Recipients) == 0 {
		return
	}

	from := cfg.FromAddr
	if from == "" {
		from = "queue-watcher@" + cfg.SMTPHost
	}
	fromDisplay := from
	if cfg.FromName != "" {
		fromDisplay = fmt.Sprintf("%s <%s>", cfg.FromName, from)
	}

	to := strings.Join(cfg.Recipients, ", ")

	headers := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n",
		fromDisplay, to, subject,
	)
	msg := []byte(headers + body)

	addr := fmt.Sprintf("%s:%d", cfg.SMTPHost, cfg.SMTPPort)

	// Try STARTTLS on port 587 / 25, plain TLS on 465.
	var err error
	if cfg.SMTPPort == 465 {
		err = sendTLS(addr, cfg, from, cfg.Recipients, msg)
	} else {
		err = sendSTARTTLS(addr, cfg, from, cfg.Recipients, msg)
	}

	if err != nil {
		log.Printf("[mailer] Failed to send email %q: %v", subject, err)
	} else {
		log.Printf("[mailer] Sent email %q to %s", subject, to)
	}
}

func sendSTARTTLS(addr string, cfg *NotifyConfig, from string, to []string, msg []byte) error {
	var auth smtp.Auth
	if cfg.Username != "" {
		host, _, _ := net.SplitHostPort(addr)
		auth = smtp.PlainAuth("", cfg.Username, cfg.Password, host)
	}
	return smtp.SendMail(addr, auth, from, to, msg)
}

func sendTLS(addr string, cfg *NotifyConfig, from string, to []string, msg []byte) error {
	host, _, _ := net.SplitHostPort(addr)
	tlsCfg := &tls.Config{ServerName: host}

	conn, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer client.Quit()

	if cfg.Username != "" {
		auth := smtp.PlainAuth("", cfg.Username, cfg.Password, host)
		if err := client.Auth(auth); err != nil {
			return err
		}
	}

	if err := client.Mail(from); err != nil {
		return err
	}
	for _, r := range to {
		if err := client.Rcpt(r); err != nil {
			return err
		}
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	_, err = w.Write(msg)
	if err != nil {
		return err
	}
	return w.Close()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
