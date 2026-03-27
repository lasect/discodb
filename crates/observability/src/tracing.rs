use tracing::Span;

pub trait Spanned {
    fn span(&self) -> Span;
}

pub fn span_discord_request(endpoint: &str, method: &str) -> Span {
    tracing::info_span!("discord.request", endpoint = %endpoint, method = %method)
}

pub fn span_wal_append(lsn: u64, txn_id: u64) -> Span {
    tracing::debug_span!("wal.append", lsn = lsn, txn_id = txn_id)
}

pub fn span_txn_begin(txn_id: u64) -> Span {
    tracing::info_span!("txn.begin", txn_id = txn_id)
}

pub fn span_txn_commit(txn_id: u64) -> Span {
    tracing::info_span!("txn.commit", txn_id = txn_id)
}

pub fn span_storage_read(table_id: u64, row_id: u64) -> Span {
    tracing::debug_span!("storage.read", table_id = table_id, row_id = row_id)
}

pub fn span_storage_write(table_id: u64, row_id: u64) -> Span {
    tracing::debug_span!("storage.write", table_id = table_id, row_id = row_id)
}

pub fn span_executor_scan(table_id: u64) -> Span {
    tracing::debug_span!("executor.scan", table_id = table_id)
}

pub fn span_recovery(replay_lsn: u64) -> Span {
    tracing::info_span!("recovery.replay", replay_lsn = replay_lsn)
}
