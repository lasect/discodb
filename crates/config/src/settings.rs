use crate::Config;
use discodb_types::GuildId;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    ReadError(String),

    #[error("failed to parse config: {0}")]
    ParseError(String),

    #[error("missing required field: {0}")]
    MissingField(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),
}

pub type ConfigResult<T> = Result<T, ConfigError>;

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let contents =
            std::fs::read_to_string(path).map_err(|e| ConfigError::ReadError(e.to_string()))?;

        serde_json::from_str(&contents).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    pub fn from_env() -> ConfigResult<Self> {
        let mut config = Config::default();

        if let Ok(token) = std::env::var("DISCORD_BOT_TOKEN") {
            config.discord.bot_token = token;
        }

        if let Ok(guild_id) = std::env::var("DISCORD_GUILD_ID") {
            if let Ok(id) = guild_id.parse::<u64>() {
                if let Some(guild_id) = GuildId::new(id) {
                    config.discord.guild_ids.push(guild_id);
                }
            }
        }

        if let Ok(level) = std::env::var("DISCODB_LOG_LEVEL") {
            config.logging.level = level;
        }

        Ok(config)
    }

    pub fn validate(&self) -> ConfigResult<()> {
        if self.discord.bot_token.is_empty() {
            return Err(ConfigError::MissingField("bot_token".to_string()));
        }

        if self.discord.guild_ids.is_empty() {
            return Err(ConfigError::MissingField("guild_ids".to_string()));
        }

        if self.scheduler.rate_limit_per_second <= 0.0 {
            return Err(ConfigError::InvalidValue(
                "rate_limit_per_second must be positive".to_string(),
            ));
        }

        Ok(())
    }
}
