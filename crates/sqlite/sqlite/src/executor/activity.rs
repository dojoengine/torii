use chrono::{DateTime, Duration, Utc};
use indexmap::IndexMap;
use sqlx::{Sqlite, Transaction as SqlxTransaction};
use tracing::info;

use crate::executor::error::ExecutorQueryError;

pub(crate) const LOG_TARGET: &str = "torii::sqlite::executor::activity";

pub type QueryResult<T> = std::result::Result<T, ExecutorQueryError>;

// Type alias for session data: (id, session_start, session_end, action_count, actions_json)
type SessionData = (String, DateTime<Utc>, DateTime<Utc>, i32, String);

/// Update activity tracking for a transaction
pub async fn update_activity(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    caller_address: &str,
    entrypoint: &str,
    executed_at: DateTime<Utc>,
    session_timeout: u64,
    excluded_entrypoints: &std::collections::HashSet<String>,
) -> QueryResult<()> {
    // Skip excluded entrypoints
    if excluded_entrypoints.contains(entrypoint) {
        return Ok(());
    }

    // Try to find the most recent session for this caller within this world/namespace
    let last_session: Option<SessionData> = sqlx::query_as(
        "SELECT id, session_start, session_end, action_count, actions
         FROM activities
         WHERE world_address = ? AND namespace = ? AND caller_address = ?
         ORDER BY session_end DESC
         LIMIT 1",
    )
    .bind(world_address)
    .bind(namespace)
    .bind(caller_address)
    .fetch_optional(&mut **tx)
    .await?;

    match last_session {
        Some((session_id, _session_start, session_end, action_count, actions_json)) => {
            // Calculate time difference from last action
            let time_diff = executed_at.signed_duration_since(session_end);

            if time_diff.num_seconds() <= session_timeout as i64 {
                // Same session - update it
                let mut action_counts: IndexMap<String, u32> =
                    serde_json::from_str(&actions_json).unwrap_or_else(|_| IndexMap::new());

                // Increment count for this action (entrypoint)
                *action_counts.entry(entrypoint.to_string()).or_insert(0) += 1;

                sqlx::query(
                    "UPDATE activities
                     SET session_end = ?,
                         action_count = ?,
                         actions = ?,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?",
                )
                .bind(executed_at)
                .bind(action_count + 1)
                .bind(serde_json::to_string(&action_counts).unwrap_or_else(|_| "{}".to_string()))
                .bind(&session_id)
                .execute(&mut **tx)
                .await?;

                info!(
                    target: LOG_TARGET,
                    world = %world_address,
                    namespace = %namespace,
                    caller = %caller_address,
                    session_id = %session_id,
                    action_count = %(action_count + 1),
                    "Updated activity session"
                );
            } else {
                // New session - time gap exceeded
                create_new_session(
                    tx,
                    world_address,
                    namespace,
                    caller_address,
                    entrypoint,
                    executed_at,
                )
                .await?;
            }
        }
        None => {
            // First session for this caller in this world/namespace
            create_new_session(
                tx,
                world_address,
                namespace,
                caller_address,
                entrypoint,
                executed_at,
            )
            .await?;
        }
    }

    Ok(())
}

async fn create_new_session(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    caller_address: &str,
    entrypoint: &str,
    executed_at: DateTime<Utc>,
) -> QueryResult<()> {
    let session_id = format!(
        "{}:{}:{}:{}",
        world_address,
        namespace,
        caller_address,
        executed_at.timestamp()
    );

    // Initialize IndexMap with first action (entrypoint)
    let mut action_counts = IndexMap::new();
    action_counts.insert(entrypoint.to_string(), 1u32);

    let actions_json = serde_json::to_string(&action_counts).unwrap_or_else(|_| "{}".to_string());

    sqlx::query(
        "INSERT INTO activities
         (id, world_address, namespace, caller_address, session_start, session_end, action_count, actions)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
    )
    .bind(&session_id)
    .bind(world_address)
    .bind(namespace)
    .bind(caller_address)
    .bind(executed_at)
    .bind(executed_at)
    .bind(&actions_json)
    .execute(&mut **tx)
    .await?;

    info!(
        target: LOG_TARGET,
        world = %world_address,
        namespace = %namespace,
        caller = %caller_address,
        session_id = %session_id,
        "Created new activity session"
    );

    Ok(())
}

/// Clean up old activity records (optional maintenance function)
pub async fn cleanup_old_activities(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    days_to_keep: i64,
) -> QueryResult<u64> {
    let cutoff_date = Utc::now() - Duration::days(days_to_keep);

    let result = sqlx::query(
        "DELETE FROM activities
         WHERE session_end < ?",
    )
    .bind(cutoff_date)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}
