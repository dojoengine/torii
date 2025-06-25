use std::str::FromStr;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use sqlx::{Column, Row, TypeInfo};
use starknet::core::types::U256;
use starknet_crypto::Felt;

use crate::constants::SQL_FELT_DELIMITER;

pub fn must_utc_datetime_from_timestamp(timestamp: u64) -> DateTime<Utc> {
    let naive_dt = DateTime::from_timestamp(timestamp as i64, 0)
        .expect("Failed to convert timestamp to NaiveDateTime");
    naive_dt.to_utc()
}

pub fn utc_dt_string_from_timestamp(timestamp: u64) -> String {
    must_utc_datetime_from_timestamp(timestamp).to_rfc3339()
}

pub fn felts_to_sql_string(felts: &[Felt]) -> String {
    felts
        .iter()
        .map(|k| format!("{:#x}", k))
        .collect::<Vec<String>>()
        .join(SQL_FELT_DELIMITER)
        + SQL_FELT_DELIMITER
}

pub fn felt_to_sql_string(felt: &Felt) -> String {
    format!("{:#x}", felt)
}

pub fn felt_and_u256_to_sql_string(felt: &Felt, u256: &U256) -> String {
    format!("{}:{}", felt_to_sql_string(felt), u256_to_sql_string(u256))
}

pub fn u256_to_sql_string(u256: &U256) -> String {
    format!("{:#064x}", u256)
}

pub fn sql_string_to_u256(sql_string: &str) -> U256 {
    let sql_string = sql_string.strip_prefix("0x").unwrap_or(sql_string);
    U256::from(crypto_bigint::U256::from_be_hex(sql_string))
}

pub fn build_keys_pattern(clause: &torii_proto::KeysClause) -> String {
    const KEY_PATTERN: &str = "0x[0-9a-fA-F]+";

    let keys = if clause.keys.is_empty() {
        vec![KEY_PATTERN.to_string()]
    } else {
        clause
            .keys
            .iter()
            .map(|felt| {
                if let Some(felt) = felt {
                    format!("{:#x}", felt)
                } else {
                    KEY_PATTERN.to_string()
                }
            })
            .collect::<Vec<_>>()
    };
    let mut keys_pattern = format!("^{}", keys.join("/"));

    if clause.pattern_matching == torii_proto::PatternMatching::VariableLen {
        keys_pattern += &format!("(/{})*", KEY_PATTERN);
    }
    keys_pattern += "/$";

    keys_pattern
}

pub fn sql_string_to_felts(sql_string: &str) -> Vec<Felt> {
    sql_string
        .split(SQL_FELT_DELIMITER)
        .map(|felt| Felt::from_str(felt).unwrap())
        .collect()
}

// Map a SQLite row to a JSON value
pub fn map_row_to_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let value: serde_json::Value = match column.type_info().name() {
            "TEXT" => row
                .get::<Option<String>, _>(i)
                .map_or(serde_json::Value::Null, serde_json::Value::String),
            "INTEGER" => row
                .get::<Option<i64>, _>(i)
                .map_or(serde_json::Value::Null, |n| {
                    serde_json::Value::Number(n.into())
                }),
            "REAL" => row
                .get::<Option<f64>, _>(i)
                .map_or(serde_json::Value::Null, |f| {
                    serde_json::Number::from_f64(f)
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                }),
            "BLOB" => row
                .get::<Option<Vec<u8>>, _>(i)
                .map_or(serde_json::Value::Null, |bytes| {
                    serde_json::Value::String(STANDARD.encode(bytes))
                }),
            _ => {
                // Try different types in order
                if let Ok(val) = row.try_get::<i64, _>(i) {
                    serde_json::Value::Number(val.into())
                } else if let Ok(val) = row.try_get::<f64, _>(i) {
                    serde_json::json!(val)
                } else if let Ok(val) = row.try_get::<bool, _>(i) {
                    serde_json::Value::Bool(val)
                } else if let Ok(val) = row.try_get::<String, _>(i) {
                    serde_json::Value::String(val)
                } else {
                    // Handle or fallback to BLOB as base64
                    let val = row.get::<Option<Vec<u8>>, _>(i);
                    val.map_or(serde_json::Value::Null, |bytes| {
                        serde_json::Value::String(STANDARD.encode(bytes))
                    })
                }
            }
        };
        obj.insert(column.name().to_string(), value);
    }
    serde_json::Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

    use super::*;

    #[test]
    fn test_must_utc_datetime_from_timestamp() {
        let timestamp = 1633027200;
        let expected_date = NaiveDate::from_ymd_opt(2021, 9, 30).unwrap();
        let expected_time = NaiveTime::from_hms_opt(18, 40, 0).unwrap();
        let expected =
            DateTime::<Utc>::from_naive_utc_and_offset(expected_date.and_time(expected_time), Utc);
        let out = must_utc_datetime_from_timestamp(timestamp);
        assert_eq!(out, expected, "Failed to convert timestamp to DateTime");
    }

    #[test]
    #[should_panic(expected = "Failed to convert timestamp to NaiveDateTime")]
    fn test_must_utc_datetime_from_timestamp_incorrect_timestamp() {
        let timestamp = i64::MAX as u64 + 1;
        let _result = must_utc_datetime_from_timestamp(timestamp);
    }

    #[test]
    fn test_utc_dt_string_from_timestamp() {
        let timestamp = 1633027200;
        let expected = "2021-09-30T18:40:00+00:00";
        let out = utc_dt_string_from_timestamp(timestamp);
        println!("{}", out);
        assert_eq!(out, expected, "Failed to convert timestamp to String");
    }
}
