use discodb_types::{ChannelId, GuildId, MessageId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscordChannel {
    pub id: ChannelId,
    pub guild_id: Option<GuildId>,
    pub name: String,
    pub kind: ChannelType,
    pub parent_id: Option<ChannelId>,
    pub topic: Option<String>,
    pub position: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChannelType {
    GuildText,
    GuildVoice,
    GuildCategory,
    GuildNews,
    GuildStore,
    GuildThread,
    GuildNewsThread,
    GuildPrivateThread,
    GuildPublicThread,
    GuildStageVoice,
    GuildForum,
}
