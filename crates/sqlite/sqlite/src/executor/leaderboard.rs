use dojo_types::schema::Ty;
use sqlx::{Sqlite, Transaction as SqlxTransaction};
use torii_sqlite_types::{LeaderboardConfig, ScoreStrategy};
use tracing::{info, warn};

use crate::executor::error::ExecutorQueryError;

pub(crate) const LOG_TARGET: &str = "torii::sqlite::executor::leaderboard";

pub type QueryResult<T> = std::result::Result<T, ExecutorQueryError>;

/// Updates a leaderboard entry based on the configured strategy
pub async fn update_leaderboard(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    leaderboard_config: &LeaderboardConfig,
    entity: &Ty,
    model_id: &str,
) -> QueryResult<()> {
    // Extract target field (e.g., player address) from the model
    let target_id = match extract_field_value(entity, &leaderboard_config.target_path) {
        Some(val) => val,
        None => {
            warn!(
                target: LOG_TARGET,
                target_path = %leaderboard_config.target_path,
                model = %entity.name(),
                "Could not extract target field from model for leaderboard"
            );
            return Ok(());
        }
    };

    let leaderboard_entry_id = format!("{}:{}", leaderboard_config.leaderboard_id, target_id);

    // Calculate score based on strategy
    let new_score = match &leaderboard_config.score_strategy {
        ScoreStrategy::Latest(field_path) => calculate_latest_score(entity, field_path)?,
        ScoreStrategy::Increment => calculate_incremented_score(tx, &leaderboard_entry_id).await?,
        ScoreStrategy::Max(field_path) => {
            calculate_max_score(tx, entity, field_path, &leaderboard_entry_id).await?
        }
        ScoreStrategy::Min(field_path) => {
            calculate_min_score(tx, entity, field_path, &leaderboard_entry_id).await?
        }
        ScoreStrategy::Sum(field_path) => {
            calculate_sum_score(tx, entity, field_path, &leaderboard_entry_id).await?
        }
    };

    // Upsert the leaderboard entry
    upsert_leaderboard_entry(
        tx,
        &leaderboard_entry_id,
        &leaderboard_config.leaderboard_id,
        &target_id,
        &new_score,
        model_id,
    )
    .await?;

    info!(
        target: LOG_TARGET,
        leaderboard_id = %leaderboard_config.leaderboard_id,
        target = %target_id,
        score = %new_score,
        strategy = %format!("{:?}", leaderboard_config.score_strategy).split('(').next().unwrap_or("unknown"),
        "Updated leaderboard entry"
    );

    Ok(())
}

/// Extract and return the latest value from a field
fn calculate_latest_score(entity: &Ty, field_path: &str) -> QueryResult<String> {
    extract_field_value(entity, field_path).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })
}

/// Increment the existing score by 1, or start at 1 if no existing score
async fn calculate_incremented_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entry_id: &str,
) -> QueryResult<String> {
    let existing_score: Option<String> =
        sqlx::query_scalar("SELECT score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    Ok(if let Some(score_str) = existing_score {
        let current: i64 = score_str.parse().unwrap_or(0);
        (current + 1).to_string()
    } else {
        "1".to_string()
    })
}

/// Keep the maximum value between existing and new
async fn calculate_max_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<String> {
    let new_value = extract_field_value(entity, field_path).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    let existing_score: Option<String> =
        sqlx::query_scalar("SELECT score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    Ok(if let Some(existing) = existing_score {
        let existing_val: f64 = existing.parse().unwrap_or(f64::MIN);
        let new_val: f64 = new_value.parse().unwrap_or(f64::MIN);
        if new_val > existing_val {
            new_value
        } else {
            existing
        }
    } else {
        new_value
    })
}

/// Keep the minimum value between existing and new
async fn calculate_min_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<String> {
    let new_value = extract_field_value(entity, field_path).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    let existing_score: Option<String> =
        sqlx::query_scalar("SELECT score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    Ok(if let Some(existing) = existing_score {
        let existing_val: f64 = existing.parse().unwrap_or(f64::MAX);
        let new_val: f64 = new_value.parse().unwrap_or(f64::MAX);
        if new_val < existing_val {
            new_value
        } else {
            existing
        }
    } else {
        new_value
    })
}

/// Sum/accumulate the new value with existing
async fn calculate_sum_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<String> {
    let new_value = extract_field_value(entity, field_path).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    let existing_score: Option<String> =
        sqlx::query_scalar("SELECT score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    Ok(if let Some(existing) = existing_score {
        let existing_val: f64 = existing.parse().unwrap_or(0.0);
        let new_val: f64 = new_value.parse().unwrap_or(0.0);
        (existing_val + new_val).to_string()
    } else {
        new_value
    })
}

/// Upsert a leaderboard entry into the database
async fn upsert_leaderboard_entry(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entry_id: &str,
    leaderboard_id: &str,
    entity_id: &str,
    score: &str,
    model_id: &str,
) -> QueryResult<()> {
    sqlx::query(
        "INSERT INTO leaderboard (id, leaderboard_id, entity_id, score, model_id) \
         VALUES (?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
         score=EXCLUDED.score, \
         model_id=EXCLUDED.model_id, \
         updated_at=CURRENT_TIMESTAMP",
    )
    .bind(entry_id)
    .bind(leaderboard_id)
    .bind(entity_id)
    .bind(score)
    .bind(model_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// Helper function to extract a field value from a Ty by path (e.g., "player" or "stats.score")
fn extract_field_value(ty: &Ty, path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    extract_field_value_recursive(ty, &parts, 0)
}

fn extract_field_value_recursive(ty: &Ty, parts: &[&str], index: usize) -> Option<String> {
    if index >= parts.len() {
        return None;
    }

    let current_part = parts[index];

    match ty {
        Ty::Struct(s) => {
            for member in &s.children {
                if member.name == current_part {
                    if index == parts.len() - 1 {
                        // Last part, return the value
                        return ty_to_sql_string(&member.ty);
                    } else {
                        // Continue traversing
                        return extract_field_value_recursive(&member.ty, parts, index + 1);
                    }
                }
            }
            None
        }
        Ty::Primitive(p) => {
            if index == parts.len() - 1 {
                Some(p.to_sql_value())
            } else {
                None
            }
        }
        Ty::Enum(e) => {
            if index == parts.len() - 1 {
                Some(e.to_sql_value())
            } else {
                None
            }
        }
        Ty::ByteArray(b) => {
            if index == parts.len() - 1 {
                Some(b.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper to convert Ty to SQL string
fn ty_to_sql_string(ty: &Ty) -> Option<String> {
    match ty {
        Ty::Primitive(p) => Some(p.to_sql_value()),
        Ty::Enum(e) => Some(e.to_sql_value()),
        Ty::ByteArray(b) => Some(b.clone()),
        _ => None,
    }
}
