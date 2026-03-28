package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"discodb/config"
	"discodb/observability"
	"discodb/wire"
)

func main() {
	addr := flag.String("addr", ":55432", "listen address for the PostgreSQL-compatible wire server")
	configPath := flag.String("config", "", "path to a JSON config file; falls back to DISCODB_CONFIG or environment-only defaults")
	serve := flag.Bool("serve", false, "start the wire server")
	checkConfig := flag.Bool("check-config", false, "validate the effective configuration")
	printConfig := flag.Bool("print-config", false, "print the effective configuration as JSON")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logger := observability.NewLogger(cfg.Logging)

	if *checkConfig {
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
			os.Exit(1)
		}
		logger.Info("configuration is valid")
	}

	if *printConfig {
		data, err := cfg.MarshalPrettyJSON()
		if err != nil {
			fmt.Fprintf(os.Stderr, "render config: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(data))
	}

	if *checkConfig || *printConfig {
		if !*serve {
			return
		}
	}

	if !*serve {
		logger.Info(
			"discodb Go workspace ready",
			slog.String("hint", "run with -serve to start the wire server"),
			slog.String("config_path", *configPath),
		)
		return
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	server := wire.NewServer(*addr, logger)
	logger.Info("starting wire server", slog.String("addr", *addr))
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "wire server failed: %v\n", err)
		os.Exit(1)
	}
}
