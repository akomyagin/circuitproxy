// Command circuitproxy is the CLI entry point for the CircuitProxy reverse proxy.
//
// Этап 0 skeleton: it parses the -config flag, loads the JSON config, and prints
// the parsed result. The HTTP server is wired up starting from Этап 1.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/akomyagin/circuitproxy/internal/config"
)

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

	// TODO(Этап 1): build the balancer, wire httputil.ReverseProxy, start the
	// http.Server on cfg.Listen.
	// TODO(Этап 2): start the health-check loop under a cancellable context.
	// TODO(Этап 5): structured slog logging of breaker transitions + /metrics.
	slog.Info("config loaded",
		"listen", cfg.Listen,
		"backends", len(cfg.Backends),
	)
	fmt.Printf("circuitproxy Этап 0: loaded %d backend(s), listen=%q (server not started yet)\n",
		len(cfg.Backends), cfg.Listen)
	return nil
}
