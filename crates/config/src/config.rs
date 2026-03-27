use discodb_types::{ChannelId, GuildId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub discord: DiscordConfig,
    pub storage: StorageConfig,
    pub scheduler: SchedulerConfig,
    pub features: FeatureFlags,
    pub logging: LoggingConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscordConfig {
    pub bot_token: String,
    pub guild_ids: Vec<GuildId>,
    pub boot_channel_name: String,
    pub request_timeout_secs: u64,
    pub max_retries: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    pub catalog_category_prefix: String,
    pub wal_channel_name: String,
    pub heap_channel_prefix: String,
    pub fsm_role_prefix: String,
    pub index_channel_prefix: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub wal_queue_size: usize,
    pub heap_queue_size: usize,
    pub index_queue_size: usize,
    pub catalog_queue_size: usize,
    pub rate_limit_burst: u32,
    pub rate_limit_per_second: f64,
    pub retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct FeatureFlags {
    pub enable_forum_indexes: bool,
    pub enable_mvcc: bool,
    pub enable_fsm: bool,
    pub enable_reactions: bool,
    pub enable_blobs: bool,
    pub enable_multi_guild: bool,
    pub enable_distributed_locks: bool,
    pub phase2_indexes: bool,
    pub phase3_multi_guild: bool,
}

impl FeatureFlags {
    pub fn mvp() -> Self {
        Self {
            enable_forum_indexes: false,
            enable_mvcc: true,
            enable_fsm: false,
            enable_reactions: true,
            enable_blobs: false,
            enable_multi_guild: false,
            enable_distributed_locks: false,
            phase2_indexes: false,
            phase3_multi_guild: false,
        }
    }

    pub fn phase2() -> Self {
        Self {
            enable_forum_indexes: true,
            enable_mvcc: true,
            enable_fsm: true,
            enable_reactions: true,
            enable_blobs: true,
            enable_multi_guild: false,
            enable_distributed_locks: false,
            phase2_indexes: true,
            phase3_multi_guild: false,
        }
    }

    pub fn phase3() -> Self {
        Self {
            enable_forum_indexes: true,
            enable_mvcc: true,
            enable_fsm: true,
            enable_reactions: true,
            enable_blobs: true,
            enable_multi_guild: true,
            enable_distributed_locks: true,
            phase2_indexes: true,
            phase3_multi_guild: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
    pub output: LogOutput,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum LogOutput {
    #[default]
    Stdout,
    File(PathBuf),
    Both(PathBuf),
}

impl Default for Config {
    fn default() -> Self {
        Self {
            discord: DiscordConfig {
                bot_token: String::new(),
                guild_ids: Vec::new(),
                boot_channel_name: "#discorddb-boot".to_string(),
                request_timeout_secs: 30,
                max_retries: 3,
            },
            storage: StorageConfig {
                catalog_category_prefix: "discodb-catalog".to_string(),
                wal_channel_name: "discodb-wal".to_string(),
                heap_channel_prefix: "discodb-heap".to_string(),
                fsm_role_prefix: "discodb-fsm".to_string(),
                index_channel_prefix: "discodb-index".to_string(),
            },
            scheduler: SchedulerConfig {
                wal_queue_size: 1000,
                heap_queue_size: 500,
                index_queue_size: 200,
                catalog_queue_size: 100,
                rate_limit_burst: 50,
                rate_limit_per_second: 50.0,
                retry_delay_ms: 100,
                max_retry_delay_ms: 30000,
            },
            features: FeatureFlags::mvp(),
            logging: LoggingConfig {
                level: "info".to_string(),
                format: LogFormat::Pretty,
                output: LogOutput::Stdout,
            },
        }
    }
}
