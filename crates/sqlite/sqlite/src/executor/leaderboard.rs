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
    let target_id = match extract_field_value(entity, &leaderboard_config.target_path, false) {
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

    // Calculate score based on strategy - returns (normalized_score, display_score)
    let (normalized_score, display_score) = match &leaderboard_config.score_strategy {
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
        &normalized_score,
        &display_score,
        model_id,
    )
    .await?;

    info!(
        target: LOG_TARGET,
        leaderboard_id = %leaderboard_config.leaderboard_id,
        target = %target_id,
        display_score = %display_score,
        strategy = %format!("{:?}", leaderboard_config.score_strategy).split('(').next().unwrap_or("unknown"),
        "Updated leaderboard entry"
    );

    Ok(())
}

/// Extract and return the latest value from a field
/// Returns (normalized_score_for_ordering, display_score_for_output)
fn calculate_latest_score(entity: &Ty, field_path: &str) -> QueryResult<(String, String)> {
    let display_score = extract_field_value(entity, field_path, false).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;
    
    let normalized_score = extract_field_value(entity, field_path, true).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;
    
    Ok((normalized_score, display_score))
}

/// Increment the existing score by 1, or start at 1 if no existing score
/// Returns (normalized_score, display_score)
async fn calculate_incremented_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entry_id: &str,
) -> QueryResult<(String, String)> {
    let existing_display: Option<String> =
        sqlx::query_scalar("SELECT display_score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    let new_count = if let Some(score_str) = existing_display {
        let current: i64 = score_str.parse().unwrap_or(0);
        current + 1
    } else {
        1
    };
    
    let display_score = new_count.to_string();
    let normalized_score = normalize_score_to_hex(&display_score);
    
    Ok((normalized_score, display_score))
}

/// Keep the maximum value between existing and new
/// Uses lexicographic comparison on zero-padded hex strings
/// Returns (normalized_score, display_score)
async fn calculate_max_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<(String, String)> {
    let new_display = extract_field_value(entity, field_path, false).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;
    let new_normalized = extract_field_value(entity, field_path, true).unwrap();

    let existing: Option<(String, String)> =
        sqlx::query_as("SELECT score, display_score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    Ok(if let Some((existing_norm, existing_disp)) = existing {
        // Lexicographic comparison on zero-padded hex strings preserves numerical ordering
        if new_normalized > existing_norm {
            (new_normalized, new_display)
        } else {
            (existing_norm, existing_disp)
        }
    } else {
        (new_normalized, new_display)
    })
}

/// Keep the minimum value between existing and new
/// Uses lexicographic comparison on zero-padded hex strings
/// Returns (normalized_score, display_score)
async fn calculate_min_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<(String, String)> {
    let new_display = extract_field_value(entity, field_path, false).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;
    let new_normalized = extract_field_value(entity, field_path, true).unwrap();

    let existing: Option<(String, String)> =
        sqlx::query_as("SELECT score, display_score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    Ok(if let Some((existing_norm, existing_disp)) = existing {
        // Lexicographic comparison on zero-padded hex strings preserves numerical ordering
        if new_normalized < existing_norm {
            (new_normalized, new_display)
        } else {
            (existing_norm, existing_disp)
        }
    } else {
        (new_normalized, new_display)
    })
}

/// Sum/accumulate the new value with existing
/// Parses display scores, adds them, and converts to both formats
/// Returns (normalized_score, display_score)
async fn calculate_sum_score(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<(String, String)> {
    let new_display = extract_field_value(entity, field_path, false).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    let existing_display: Option<String> =
        sqlx::query_scalar("SELECT display_score FROM leaderboard WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    let sum_value = if let Some(existing) = existing_display {
        // Parse display values and add them
        let existing_val = parse_display_to_i128(&existing).unwrap_or(0);
        let new_val = parse_display_to_i128(&new_display).unwrap_or(0);
        existing_val.saturating_add(new_val)
    } else {
        parse_display_to_i128(&new_display).unwrap_or(0)
    };
    
    let display_score = sum_value.to_string();
    let normalized_score = normalize_score_to_hex(&display_score);
    
    Ok((normalized_score, display_score))
}

/// Helper to parse a display score (decimal or hex) to i128
fn parse_display_to_i128(display_str: &str) -> Option<i128> {
    if let Some(hex_str) = display_str.strip_prefix("0x") {
        // Parse as hex
        u128::from_str_radix(hex_str, 16).ok().map(|v| v as i128)
    } else {
        // Parse as decimal
        display_str.parse::<i128>().ok()
    }
}

/// Upsert a leaderboard entry into the database
async fn upsert_leaderboard_entry(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entry_id: &str,
    leaderboard_id: &str,
    entity_id: &str,
    normalized_score: &str,
    display_score: &str,
    model_id: &str,
) -> QueryResult<()> {
    sqlx::query(
        "INSERT INTO leaderboard (id, leaderboard_id, entity_id, score, display_score, model_id) \
         VALUES (?, ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
         score=EXCLUDED.score, \
         display_score=EXCLUDED.display_score, \
         model_id=EXCLUDED.model_id, \
         updated_at=CURRENT_TIMESTAMP",
    )
    .bind(entry_id)
    .bind(leaderboard_id)
    .bind(entity_id)
    .bind(normalized_score)
    .bind(display_score)
    .bind(model_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// Helper function to extract a field value from a Ty by path (e.g., "player" or "stats.score")
/// If normalize is true, returns zero-padded hex with bias for ordering
/// If normalize is false, returns raw value for display
fn extract_field_value(ty: &Ty, path: &str, normalize: bool) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    extract_field_value_recursive(ty, &parts, 0, normalize)
}

fn extract_field_value_recursive(ty: &Ty, parts: &[&str], index: usize, normalize: bool) -> Option<String> {
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
                        return ty_to_sql_string(&member.ty, normalize);
                    } else {
                        // Continue traversing
                        return extract_field_value_recursive(&member.ty, parts, index + 1, normalize);
                    }
                }
            }
            None
        }
        Ty::Primitive(p) => {
            if index == parts.len() - 1 {
                let raw_value = p.to_sql_value();
                Some(if normalize { normalize_score_to_hex(&raw_value) } else { raw_value })
            } else {
                None
            }
        }
        Ty::Enum(e) => {
            if index == parts.len() - 1 {
                let raw_value = e.to_sql_value();
                Some(if normalize { normalize_score_to_hex(&raw_value) } else { raw_value })
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
/// If normalize is true, converts to zero-padded hex for consistent ordering
/// If normalize is false, returns raw to_sql_value() for display
fn ty_to_sql_string(ty: &Ty, normalize: bool) -> Option<String> {
    match ty {
        Ty::Primitive(p) => {
            let sql_value = p.to_sql_value();
            Some(if normalize { normalize_score_to_hex(&sql_value) } else { sql_value })
        }
        Ty::Enum(e) => {
            let sql_value = e.to_sql_value();
            Some(if normalize { normalize_score_to_hex(&sql_value) } else { sql_value })
        }
        Ty::ByteArray(b) => Some(b.clone()),
        _ => None,
    }
}

/// Normalize score values to zero-padded hex strings for consistent lexicographic ordering
/// All values are padded to 64 hex characters (32 bytes) to handle up to u256
/// This ensures:
/// - Lexicographic ordering matches numerical ordering (0x0000...0009 < 0x0000...0064 < 0x0000...00ff)
/// - No overflow issues with large numbers (u256 fits in 64 hex chars)
/// - No precision loss from REAL conversion
/// 
/// For signed integers, we add a bias to shift into positive range for correct lexicographic ordering:
/// i128::MIN + bias = 0, i128::MAX + bias = u128::MAX
fn normalize_score_to_hex(value: &str) -> String {
    if let Some(hex_str) = value.strip_prefix("0x") {
        // Already hex, assume it's unsigned and zero-pad to 64 chars
        format!("0x{:0>64}", hex_str)
    } else {
        // Decimal string, parse and convert to hex
        // Try as u128 first (most common case - unsigned/positive scores)
        if let Ok(num) = value.parse::<u128>() {
            return format!("0x{:0>64x}", num);
        }
        // Try as i128 for signed scores (less common)
        if let Ok(num) = value.parse::<i128>() {
            // Add bias to shift into positive range for correct lexicographic ordering
            // i128::MIN (-2^127) becomes 0, i128::MAX (2^127-1) becomes u128::MAX
            const BIAS: u128 = 1u128 << 127; // 2^127
            let biased = (num as u128).wrapping_add(BIAS);
            return format!("0x{:0>64x}", biased);
        }
        // If parsing fails, return a zero-padded zero as fallback
        format!("0x{:0>64}", "0")
    }
}
