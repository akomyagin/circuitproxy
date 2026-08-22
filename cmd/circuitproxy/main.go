// Command circuitproxy is the CLI entry point for the CircuitProxy reverse proxy.
//
// It parses the -config flag, loads the JSON config, builds the round-robin
// balancer and serves HTTP on the configured listen address until SIGINT or
// SIGTERM, then shuts down gracefully.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akomyagin/circuitproxy/internal/config"
	"github.com/akomyagin/circuitproxy/internal/proxy"
)

// shutdownTimeout bounds how long in-flight requests may drain on shutdown.
const shutdownTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("circuitproxy failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.json", "path to the JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	balancer, err := proxy.NewBalancer(cfg)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: balancer.Handler(),
	}

	// ctx is cancelled on SIGINT/SIGTERM; Этап 2 will also hang the
	// health-check loop off this context.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// TODO(Этап 2): start the health-check loop under ctx.
	// TODO(Этап 5): structured slog logging of breaker transitions + /metrics.

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	slog.Info("circuitproxy started",
		"listen", cfg.Listen,
		"backends", len(cfg.Backends),
	)

	select {
	case err := <-errCh:
		// ListenAndServe failed on its own (e.g. address already in use).
		return err
	case <-ctx.Done():
	}

	slog.Info("shutdown signal received, draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	slog.Info("circuitproxy stopped")
	return nil
}
