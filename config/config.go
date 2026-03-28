package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"discodb/types"
)

var (
	ErrMissingField = errors.New("missing required field")
	ErrInvalidValue = errors.New("invalid value")
)

type Config struct {
	Discord   DiscordConfig   `json:"discord"`
	Storage   StorageConfig   `json:"storage"`
	Scheduler SchedulerConfig `json:"scheduler"`
	Features  FeatureFlags    `json:"features"`
	Logging   LoggingConfig   `json:"logging"`
}

type DiscordConfig struct {
	Tokens             DiscordTokenConfig `json:"tokens"`
	GuildIDs           []types.GuildID    `json:"guild_ids"`
	BootChannelName    string             `json:"boot_channel_name"`
	RequestTimeoutSecs uint64             `json:"request_timeout_secs"`
	MaxRetries         uint32             `json:"max_retries"`
}

type DiscordTokenConfig struct {
	WAL      string `json:"wal"`
	Heap     string `json:"heap"`
	Index    string `json:"index"`
	Catalog  string `json:"catalog"`
	Overflow string `json:"overflow"`
}

type StorageConfig struct {
	CatalogCategoryPrefix string `json:"catalog_category_prefix"`
	WALChannelName        string `json:"wal_channel_name"`
	HeapChannelPrefix     string `json:"heap_channel_prefix"`
	FSMRolePrefix         string `json:"fsm_role_prefix"`
	IndexChannelPrefix    string `json:"index_channel_prefix"`
}

type SchedulerConfig struct {
	WALQueueSize       int     `json:"wal_queue_size"`
	HeapQueueSize      int     `json:"heap_queue_size"`
	IndexQueueSize     int     `json:"index_queue_size"`
	CatalogQueueSize   int     `json:"catalog_queue_size"`
	OverflowQueueSize  int     `json:"overflow_queue_size"`
	RateLimitBurst     uint32  `json:"rate_limit_burst"`
	RateLimitPerSecond float64 `json:"rate_limit_per_second"`
	RetryDelayMS       uint64  `json:"retry_delay_ms"`
	MaxRetryDelayMS    uint64  `json:"max_retry_delay_ms"`
}

type FeatureFlags struct {
	EnableForumIndexes     bool `json:"enable_forum_indexes"`
	EnableMVCC             bool `json:"enable_mvcc"`
	EnableFSM              bool `json:"enable_fsm"`
	EnableReactions        bool `json:"enable_reactions"`
	EnableBlobs            bool `json:"enable_blobs"`
	EnableMultiGuild       bool `json:"enable_multi_guild"`
	EnableDistributedLocks bool `json:"enable_distributed_locks"`
	Phase2Indexes          bool `json:"phase2_indexes"`
	Phase3MultiGuild       bool `json:"phase3_multi_guild"`
}

type LoggingConfig struct {
	Level  string    `json:"level"`
	Format LogFormat `json:"format"`
	Output LogOutput `json:"output"`
}

type LogFormat string

const (
	LogFormatPretty LogFormat = "pretty"
	LogFormatJSON   LogFormat = "json"
)

type LogOutput struct {
	Mode string `json:"mode"`
	Path string `json:"path,omitempty"`
}

func Default() Config {
	return Config{
		Discord: DiscordConfig{
			BootChannelName:    "#discodb-boot",
			RequestTimeoutSecs: 30,
			MaxRetries:         3,
		},
		Storage: StorageConfig{
			CatalogCategoryPrefix: "discodb-catalog",
			WALChannelName:        "discodb-wal",
			HeapChannelPrefix:     "discodb-heap",
			FSMRolePrefix:         "discodb-fsm",
			IndexChannelPrefix:    "discodb-index",
		},
		Scheduler: SchedulerConfig{
			WALQueueSize:       1000,
			HeapQueueSize:      500,
			IndexQueueSize:     200,
			CatalogQueueSize:   100,
			OverflowQueueSize:  100,
			RateLimitBurst:     50,
			RateLimitPerSecond: 50,
			RetryDelayMS:       100,
			MaxRetryDelayMS:    30000,
		},
		Features: MVPFlags(),
		Logging: LoggingConfig{
			Level:  "info",
			Format: LogFormatPretty,
			Output: LogOutput{Mode: "stdout"},
		},
	}
}

func MVPFlags() FeatureFlags {
	return FeatureFlags{
		EnableMVCC:      true,
		EnableReactions: true,
	}
}

func Phase2Flags() FeatureFlags {
	cfg := MVPFlags()
	cfg.EnableForumIndexes = true
	cfg.EnableFSM = true
	cfg.EnableBlobs = true
	cfg.Phase2Indexes = true
	return cfg
}

func Phase3Flags() FeatureFlags {
	cfg := Phase2Flags()
	cfg.EnableMultiGuild = true
	cfg.EnableDistributedLocks = true
	cfg.Phase3MultiGuild = true
	return cfg
}

func FromFile(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config file: %w", err)
	}
	return cfg, nil
}

func FromEnv() (Config, error) {
	cfg := Default()
	return applyEnvOverrides(cfg)
}

func Load(path string) (Config, error) {
	if path == "" {
		path = os.Getenv("DISCODB_CONFIG")
	}

	var (
		cfg Config
		err error
	)
	if path != "" {
		cfg, err = FromFile(path)
		if err != nil {
			return Config{}, err
		}
	} else {
		cfg = Default()
	}

	return applyEnvOverrides(cfg)
}

func applyEnvOverrides(cfg Config) (Config, error) {
	if token := os.Getenv("DISCORD_BOT_TOKEN"); token != "" {
		cfg.Discord.Tokens.ApplyShared(token)
	}
	if token := os.Getenv("DISCORD_BOT_TOKEN_WAL"); token != "" {
		cfg.Discord.Tokens.WAL = token
	}
	if token := os.Getenv("DISCORD_BOT_TOKEN_HEAP"); token != "" {
		cfg.Discord.Tokens.Heap = token
	}
	if token := os.Getenv("DISCORD_BOT_TOKEN_INDEX"); token != "" {
		cfg.Discord.Tokens.Index = token
	}
	if token := os.Getenv("DISCORD_BOT_TOKEN_CATALOG"); token != "" {
		cfg.Discord.Tokens.Catalog = token
	}
	if token := os.Getenv("DISCORD_BOT_TOKEN_OVERFLOW"); token != "" {
		cfg.Discord.Tokens.Overflow = token
	}
	if guildID := os.Getenv("DISCORD_GUILD_ID"); guildID != "" {
		var raw uint64
		if _, err := fmt.Sscanf(guildID, "%d", &raw); err == nil {
			id, err := types.NewGuildID(raw)
			if err != nil {
				return Config{}, fmt.Errorf("parse DISCORD_GUILD_ID: %w", err)
			}
			cfg.Discord.GuildIDs = []types.GuildID{id}
		}
	}
	if level := os.Getenv("DISCODB_LOG_LEVEL"); level != "" {
		cfg.Logging.Level = level
	}
	if output := os.Getenv("DISCODB_LOG_FILE"); output != "" {
		cfg.Logging.Output = LogOutput{Mode: "file", Path: filepath.Clean(output)}
	}
	return cfg, nil
}

func (c Config) MarshalPrettyJSON() ([]byte, error) {
	return json.MarshalIndent(c, "", "  ")
}

func (c Config) Validate() error {
	if c.Discord.Tokens.WAL == "" {
		return fmt.Errorf("%w: discord.tokens.wal", ErrMissingField)
	}
	if c.Discord.Tokens.Heap == "" {
		return fmt.Errorf("%w: discord.tokens.heap", ErrMissingField)
	}
	if c.Discord.Tokens.Index == "" {
		return fmt.Errorf("%w: discord.tokens.index", ErrMissingField)
	}
	if c.Discord.Tokens.Catalog == "" {
		return fmt.Errorf("%w: discord.tokens.catalog", ErrMissingField)
	}
	if c.Discord.Tokens.Overflow == "" {
		return fmt.Errorf("%w: discord.tokens.overflow", ErrMissingField)
	}
	if len(c.Discord.GuildIDs) == 0 {
		return fmt.Errorf("%w: discord.guild_ids", ErrMissingField)
	}
	if c.Scheduler.RateLimitPerSecond <= 0 {
		return fmt.Errorf("%w: scheduler.rate_limit_per_second", ErrInvalidValue)
	}
	if c.Scheduler.OverflowQueueSize <= 0 {
		return fmt.Errorf("%w: scheduler.overflow_queue_size", ErrInvalidValue)
	}
	return nil
}

func (c DiscordTokenConfig) All() map[string]string {
	return map[string]string{
		"wal":      c.WAL,
		"heap":     c.Heap,
		"index":    c.Index,
		"catalog":  c.Catalog,
		"overflow": c.Overflow,
	}
}

func (c *DiscordTokenConfig) ApplyShared(token string) {
	c.WAL = token
	c.Heap = token
	c.Index = token
	c.Catalog = token
	c.Overflow = token
}
