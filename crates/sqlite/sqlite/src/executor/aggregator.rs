use chrono::{DateTime, Utc};
use dojo_types::schema::Ty;
use sqlx::{Sqlite, Transaction as SqlxTransaction};
use torii_sqlite_types::{Aggregation, AggregatorConfig};
use tracing::{info, warn};

use crate::executor::error::ExecutorQueryError;

pub(crate) const LOG_TARGET: &str = "torii::sqlite::executor::aggregator";

pub type QueryResult<T> = std::result::Result<T, ExecutorQueryError>;

/// Updates an aggregation entry based on the configured strategy
/// Returns the updated entry with its calculated position, or None if the group_by field can't be extracted
pub async fn update_aggregation(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    aggregator_config: &AggregatorConfig,
    entity: &Ty,
    model_id: &str,
) -> QueryResult<Option<torii_proto::AggregationEntry>> {
    // Extract group_by field (e.g., player address) from the model
    let entity_id = match extract_field_value(entity, &aggregator_config.group_by, false) {
        Some(val) => val,
        None => {
            warn!(
                target: LOG_TARGET,
                group_by = %aggregator_config.group_by,
                model = %entity.name(),
                "Could not extract group_by field from model for aggregator"
            );
            return Ok(None);
        }
    };

    let entry_id = format!("{}:{}", aggregator_config.id, entity_id);

    // Calculate value based on aggregation strategy - returns (normalized_value, display_value, optional_metadata)
    let (normalized_value, display_value, metadata) = match &aggregator_config.aggregation {
        Aggregation::Latest(field_path) => {
            let (norm, disp) = calculate_latest_value(entity, field_path)?;
            (norm, disp, None)
        }
        Aggregation::Count => {
            let (norm, disp) = calculate_count_value(tx, &entry_id).await?;
            (norm, disp, None)
        }
        Aggregation::Max(field_path) => {
            let (norm, disp) = calculate_max_value(tx, entity, field_path, &entry_id).await?;
            (norm, disp, None)
        }
        Aggregation::Min(field_path) => {
            let (norm, disp) = calculate_min_value(tx, entity, field_path, &entry_id).await?;
            (norm, disp, None)
        }
        Aggregation::Sum(field_path) => {
            let (norm, disp) = calculate_sum_value(tx, entity, field_path, &entry_id).await?;
            (norm, disp, None)
        }
        Aggregation::Avg(field_path) => {
            calculate_avg_value(tx, entity, field_path, &entry_id).await?
        }
    };

    // Upsert the aggregation entry and get it back with position
    let aggregation_entry = upsert_aggregation_entry(
        tx,
        &entry_id,
        &aggregator_config.id,
        &entity_id,
        &normalized_value,
        &display_value,
        metadata.as_deref(),
        model_id,
    )
    .await?;

    info!(
        target: LOG_TARGET,
        aggregator_id = %aggregator_config.id,
        entity = %entity_id,
        display_value = %display_value,
        position = %aggregation_entry.position,
        aggregation = %format!("{:?}", aggregator_config.aggregation).split('(').next().unwrap_or("unknown"),
        "Updated aggregation entry"
    );

    Ok(Some(aggregation_entry))
}

/// Extract and return the latest value from a field
/// Returns (normalized_value_for_ordering, display_value_for_output)
fn calculate_latest_value(entity: &Ty, field_path: &str) -> QueryResult<(String, String)> {
    let display_value = extract_field_value(entity, field_path, false).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    let normalized_value = extract_field_value(entity, field_path, true).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    Ok((normalized_value, display_value))
}

/// Count occurrences by incrementing by 1, or start at 1 if no existing entry
/// Returns (normalized_value, display_value)
async fn calculate_count_value(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entry_id: &str,
) -> QueryResult<(String, String)> {
    let existing_display: Option<String> =
        sqlx::query_scalar("SELECT display_value FROM aggregations WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    let new_count = if let Some(value_str) = existing_display {
        let current: i64 = value_str.parse().unwrap_or(0);
        current + 1
    } else {
        1
    };

    let display_value = new_count.to_string();
    let normalized_value = normalize_value_to_hex(&display_value);

    Ok((normalized_value, display_value))
}

/// Keep the maximum value between existing and new
/// Uses lexicographic comparison on zero-padded hex strings
/// Returns (normalized_value, display_value)
async fn calculate_max_value(
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
        sqlx::query_as("SELECT value, display_value FROM aggregations WHERE id = ?")
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
/// Returns (normalized_value, display_value)
async fn calculate_min_value(
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
        sqlx::query_as("SELECT value, display_value FROM aggregations WHERE id = ?")
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
/// Parses display values, adds them, and converts to both formats
/// Returns (normalized_value, display_value)
async fn calculate_sum_value(
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
        sqlx::query_scalar("SELECT display_value FROM aggregations WHERE id = ?")
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

    let display_value = sum_value.to_string();
    let normalized_value = normalize_value_to_hex(&display_value);

    Ok((normalized_value, display_value))
}

/// Calculate average value
/// Stores sum and count in metadata field as JSON: {"sum": "123", "count": 5}
/// Returns (normalized_average, display_average, metadata_json)
async fn calculate_avg_value(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entity: &Ty,
    field_path: &str,
    entry_id: &str,
) -> QueryResult<(String, String, Option<String>)> {
    let new_display = extract_field_value(entity, field_path, false).ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(format!(
            "Could not extract field '{}' from model '{}'",
            field_path,
            entity.name()
        ))
    })?;

    // Fetch existing metadata
    let existing_metadata: Option<String> =
        sqlx::query_scalar("SELECT metadata FROM aggregations WHERE id = ?")
            .bind(entry_id)
            .fetch_optional(&mut **tx)
            .await?;

    // Parse existing sum and count
    let (existing_sum, existing_count) = if let Some(meta) = existing_metadata {
        // Parse JSON: {"sum": "123", "count": 5}
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&meta) {
            let sum_str = parsed["sum"].as_str().unwrap_or("0");
            let count = parsed["count"].as_i64().unwrap_or(0);
            (parse_display_to_i128(sum_str).unwrap_or(0), count)
        } else {
            (0, 0)
        }
    } else {
        (0, 0)
    };

    // Add new value
    let new_val = parse_display_to_i128(&new_display).unwrap_or(0);
    let new_sum = existing_sum.saturating_add(new_val);
    let new_count = existing_count + 1;

    // Calculate average
    let avg_value = if new_count > 0 {
        new_sum / (new_count as i128)
    } else {
        0
    };

    let display_value = avg_value.to_string();
    let normalized_value = normalize_value_to_hex(&display_value);

    // Store metadata for next calculation
    let metadata = format!(r#"{{"sum":"{}","count":{}}}"#, new_sum, new_count);

    Ok((normalized_value, display_value, Some(metadata)))
}

/// Helper to parse a display value (decimal or hex) to i128
fn parse_display_to_i128(display_str: &str) -> Option<i128> {
    if let Some(hex_str) = display_str.strip_prefix("0x") {
        // Parse as hex
        u128::from_str_radix(hex_str, 16).ok().map(|v| v as i128)
    } else {
        // Parse as decimal
        display_str.parse::<i128>().ok()
    }
}

/// Upsert an aggregation entry into the database
#[allow(clippy::too_many_arguments)]
async fn upsert_aggregation_entry(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    entry_id: &str,
    aggregator_id: &str,
    entity_id: &str,
    normalized_value: &str,
    display_value: &str,
    metadata: Option<&str>,
    model_id: &str,
) -> QueryResult<torii_proto::AggregationEntry> {
    // First, upsert the entry
    sqlx::query(
        "INSERT INTO aggregations (id, aggregator_id, entity_id, value, display_value, metadata, model_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
         value=EXCLUDED.value, \
         display_value=EXCLUDED.display_value, \
         metadata=EXCLUDED.metadata, \
         model_id=EXCLUDED.model_id, \
         updated_at=CURRENT_TIMESTAMP",
    )
    .bind(entry_id)
    .bind(aggregator_id)
    .bind(entity_id)
    .bind(normalized_value)
    .bind(display_value)
    .bind(metadata)
    .bind(model_id)
    .execute(&mut **tx)
    .await?;

    // Then, fetch the entry with its calculated position
    let entry: (String, String, String, String, String, String, DateTime<Utc>, DateTime<Utc>, i64) = sqlx::query_as(
        "SELECT a.id, a.aggregator_id, a.entity_id, a.value, a.display_value, a.model_id, \
         a.created_at, a.updated_at, \
         ROW_NUMBER() OVER (PARTITION BY a.aggregator_id ORDER BY a.value DESC) as position \
         FROM aggregations a \
         WHERE a.id = ?",
    )
    .bind(entry_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(torii_proto::AggregationEntry {
        id: entry.0,
        aggregator_id: entry.1,
        entity_id: entry.2,
        value: entry.3,
        display_value: entry.4,
        model_id: entry.5,
        created_at: entry.6,
        updated_at: entry.7,
        position: entry.8 as u64,
    })
}

/// Helper function to extract a field value from a Ty by path (e.g., "player" or "stats.score")
/// If normalize is true, returns zero-padded hex with bias for ordering
/// If normalize is false, returns raw value for display
fn extract_field_value(ty: &Ty, path: &str, normalize: bool) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    extract_field_value_recursive(ty, &parts, 0, normalize)
}

fn extract_field_value_recursive(
    ty: &Ty,
    parts: &[&str],
    index: usize,
    normalize: bool,
) -> Option<String> {
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
                        return extract_field_value_recursive(
                            &member.ty,
                            parts,
                            index + 1,
                            normalize,
                        );
                    }
                }
            }
            None
        }
        Ty::Primitive(p) => {
            if index == parts.len() - 1 {
                let raw_value = p.to_sql_value();
                Some(if normalize {
                    normalize_value_to_hex(&raw_value)
                } else {
                    raw_value
                })
            } else {
                None
            }
        }
        Ty::Enum(e) => {
            if index == parts.len() - 1 {
                let raw_value = e.to_sql_value();
                Some(if normalize {
                    normalize_value_to_hex(&raw_value)
                } else {
                    raw_value
                })
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
            Some(if normalize {
                normalize_value_to_hex(&sql_value)
            } else {
                sql_value
            })
        }
        Ty::Enum(e) => {
            let sql_value = e.to_sql_value();
            Some(if normalize {
                normalize_value_to_hex(&sql_value)
            } else {
                sql_value
            })
        }
        Ty::ByteArray(b) => Some(b.clone()),
        _ => None,
    }
}

/// Normalize values to zero-padded hex strings for consistent lexicographic ordering
/// All values are padded to 64 hex characters (32 bytes) to handle up to u256
/// This ensures:
/// - Lexicographic ordering matches numerical ordering (0x0000...0009 < 0x0000...0064 < 0x0000...00ff)
/// - No overflow issues with large numbers (u256 fits in 64 hex chars)
/// - No precision loss from REAL conversion
///
/// For signed integers, we add a bias to shift into positive range for correct lexicographic ordering:
/// i128::MIN + bias = 0, i128::MAX + bias = u128::MAX
fn normalize_value_to_hex(value: &str) -> String {
    if let Some(hex_str) = value.strip_prefix("0x") {
        // Already hex, assume it's unsigned and zero-pad to 64 chars
        format!("0x{:0>64}", hex_str)
    } else {
        // Decimal string, parse and convert to hex
        // Try as u128 first (most common case - unsigned/positive values)
        if let Ok(num) = value.parse::<u128>() {
            return format!("0x{:0>64x}", num);
        }
        // Try as i128 for signed values (less common)
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
