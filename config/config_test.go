package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFromFileStartsWithDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "discodb.json")
	data := []byte(`{"discord":{"tokens":{"wal":"a","heap":"b","index":"c","catalog":"d","overflow":"e"},"guild_ids":[123]}}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := FromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.WALChannelName != "discodb-wal" {
		t.Fatalf("expected default storage values to survive partial file config, got %q", cfg.Storage.WALChannelName)
	}
	if cfg.Scheduler.OverflowQueueSize != 100 {
		t.Fatalf("expected default overflow queue size, got %d", cfg.Scheduler.OverflowQueueSize)
	}
	if len(cfg.Discord.GuildIDs) != 1 || cfg.Discord.GuildIDs[0] != 123 {
		t.Fatalf("unexpected guild ids: %#v", cfg.Discord.GuildIDs)
	}
}

func TestLoadAppliesEnvOverrides(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "env-token")
	t.Setenv("DISCORD_GUILD_ID", "456")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Discord.Tokens.WAL != "env-token" || cfg.Discord.Tokens.Overflow != "env-token" {
		t.Fatalf("expected shared token fanout, got %#v", cfg.Discord.Tokens)
	}
	if len(cfg.Discord.GuildIDs) != 1 || cfg.Discord.GuildIDs[0] != 456 {
		t.Fatalf("unexpected guild ids: %#v", cfg.Discord.GuildIDs)
	}
}

func TestLoadAppliesPerClassTokenOverrides(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "shared")
	t.Setenv("DISCORD_BOT_TOKEN_INDEX", "index-only")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Discord.Tokens.WAL != "shared" {
		t.Fatalf("unexpected wal token: %q", cfg.Discord.Tokens.WAL)
	}
	if cfg.Discord.Tokens.Index != "index-only" {
		t.Fatalf("unexpected index token: %q", cfg.Discord.Tokens.Index)
	}
}
