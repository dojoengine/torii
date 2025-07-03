use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::io::prelude::*;
use torii_proto::{OrderDirection, Pagination, PaginationDirection};

use crate::error::{Error, QueryError};

/// Compresses a string using Deflate and then encodes it using Base64 (no padding).
pub fn encode_cursor(value: &str) -> Result<String, Error> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(value.as_bytes()).map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Cursor compression error: {}",
            e
        )))
    })?;
    let compressed_bytes = encoder.finish().map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Cursor compression finish error: {}",
            e
        )))
    })?;

    Ok(BASE64_URL_SAFE_NO_PAD.encode(&compressed_bytes))
}

/// Decodes a Base64 (no padding) string and then decompresses it using Deflate.
pub fn decode_cursor(encoded_cursor: &str) -> Result<String, Error> {
    let compressed_cursor_bytes = BASE64_URL_SAFE_NO_PAD.decode(encoded_cursor).map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Base64 decode error: {}",
            e
        )))
    })?;

    let mut decoder = DeflateDecoder::new(&compressed_cursor_bytes[..]);
    let mut decompressed_str = String::new();
    decoder.read_to_string(&mut decompressed_str).map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Decompression error: {}",
            e
        )))
    })?;

    Ok(decompressed_str)
}

pub fn build_cursor_conditions(
    pagination: &Pagination,
    cursor_values: Option<&[String]>,
    table_name: &str,
) -> Result<(Vec<String>, Vec<String>), Error> {
    let mut conditions = Vec::new();
    let mut binds = Vec::new();

    if let Some(values) = cursor_values {
        let expected_len = if pagination.order_by.is_empty() {
            1
        } else {
            pagination.order_by.len() + 1
        };
        if values.len() != expected_len {
            return Err(Error::Query(QueryError::InvalidCursor(
                "Invalid cursor values length".to_string(),
            )));
        }

        if pagination.order_by.is_empty() {
            let operator = if pagination.direction == PaginationDirection::Forward {
                "<"
            } else {
                ">"
            };
            conditions.push(format!("{}.event_id {} ?", table_name, operator));
            binds.push(values[0].clone());
        } else {
            for (i, (ob, val)) in pagination.order_by.iter().zip(values).enumerate() {
                let operator = match (&ob.direction, &pagination.direction) {
                    (OrderDirection::Asc, PaginationDirection::Forward) => ">",
                    (OrderDirection::Asc, PaginationDirection::Backward) => "<",
                    (OrderDirection::Desc, PaginationDirection::Forward) => "<",
                    (OrderDirection::Desc, PaginationDirection::Backward) => ">",
                };

                let condition = if i == 0 {
                    format!("[{}] {} ?", ob.field, operator)
                } else {
                    let prev = (0..i)
                        .map(|j| {
                            let prev_ob = &pagination.order_by[j];
                            format!("[{}] = ?", prev_ob.field)
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    format!("({} AND [{}] {} ?)", prev, ob.field, operator)
                };
                conditions.push(condition);
                binds.push(val.clone());
            }
            let operator = if pagination.direction == PaginationDirection::Forward {
                "<"
            } else {
                ">"
            };
            conditions.push(format!("{}.event_id {} ?", table_name, operator));
            binds.push(values.last().unwrap().clone());
        }
    }
    Ok((conditions, binds))
}

pub fn build_cursor_values(pagination: &Pagination, row: &SqliteRow) -> Result<Vec<String>, Error> {
    if pagination.order_by.is_empty() {
        Ok(vec![row.try_get("event_id")?])
    } else {
        let mut values = Vec::new();
        for ob in &pagination.order_by {
            let col = ob.field.clone();
            // Try as String first
            match row.try_get::<String, &str>(&col) {
                Ok(val) => values.push(val),
                Err(_) => {
                    // Try as i64 (INTEGER)
                    match row.try_get::<i64, &str>(&col) {
                        Ok(val) => values.push(val.to_string()),
                        Err(e) => {
                            return Err(Error::Query(QueryError::InvalidCursor(format!(
                                "Could not extract cursor value for column {}: {}",
                                col, e
                            ))));
                        }
                    }
                }
            }
        }
        values.push(row.try_get("event_id")?);
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use torii_proto::{OrderBy, OrderDirection, PaginationDirection};

    #[test]
    fn test_encode_decode_cursor() {
        let original = "test_cursor_data";
        let encoded = encode_cursor(original).unwrap();
        let decoded = decode_cursor(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_decode_cursor_empty() {
        let original = "";
        let encoded = encode_cursor(original).unwrap();
        let decoded = decode_cursor(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_decode_cursor_special_chars() {
        let original = "test/with/slashes|and|pipes:and:colons";
        let encoded = encode_cursor(original).unwrap();
        let decoded = decode_cursor(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_invalid_cursor() {
        let result = decode_cursor("invalid_base64!");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_cursor_conditions_forward_no_order() {
        let pagination = Pagination {
            direction: PaginationDirection::Forward,
            cursor: Some("cursor".to_string()),
            limit: Some(10),
            order_by: vec![],
            offset: None,
        };
        let cursor_values = vec!["123".to_string()];
        let (conditions, binds) =
            build_cursor_conditions(&pagination, Some(&cursor_values), "entities").unwrap();

        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0], "entities.event_id < ?");
        assert_eq!(binds.len(), 1);
        assert_eq!(binds[0], "123");
    }

    #[test]
    fn test_build_cursor_conditions_backward_no_order() {
        let pagination = Pagination {
            direction: PaginationDirection::Backward,
            cursor: Some("cursor".to_string()),
            limit: Some(10),
            order_by: vec![],
            offset: None,
        };
        let cursor_values = vec!["123".to_string()];
        let (conditions, binds) =
            build_cursor_conditions(&pagination, Some(&cursor_values), "entities").unwrap();

        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0], "entities.event_id > ?");
        assert_eq!(binds.len(), 1);
        assert_eq!(binds[0], "123");
    }

    #[test]
    fn test_build_cursor_conditions_with_order_asc_forward() {
        let pagination = Pagination {
            direction: PaginationDirection::Forward,
            cursor: Some("cursor".to_string()),
            limit: Some(10),
            order_by: vec![OrderBy {
                field: "Player.score".to_string(),
                direction: OrderDirection::Asc,
            }],
            offset: None,
        };
        let cursor_values = vec!["100".to_string(), "123".to_string()];
        let (conditions, binds) =
            build_cursor_conditions(&pagination, Some(&cursor_values), "entities").unwrap();

        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], "[Player.score] > ?");
        assert_eq!(conditions[1], "entities.event_id < ?");
        assert_eq!(binds.len(), 2);
        assert_eq!(binds[0], "100");
        assert_eq!(binds[1], "123");
    }

    #[test]
    fn test_build_cursor_conditions_invalid_length() {
        let pagination = Pagination {
            direction: PaginationDirection::Forward,
            cursor: Some("cursor".to_string()),
            limit: Some(10),
            order_by: vec![],
            offset: None,
        };
        let cursor_values = vec!["123".to_string(), "456".to_string()]; // Too many values
        let result = build_cursor_conditions(&pagination, Some(&cursor_values), "entities");

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Query(QueryError::InvalidCursor(msg)) => {
                assert_eq!(msg, "Invalid cursor values length");
            }
            _ => panic!("Expected InvalidCursor error"),
        }
    }

    #[test]
    fn test_build_cursor_conditions_no_cursor() {
        let pagination = Pagination {
            direction: PaginationDirection::Forward,
            cursor: None,
            limit: Some(10),
            order_by: vec![],
            offset: None,
        };
        let (conditions, binds) = build_cursor_conditions(&pagination, None, "entities").unwrap();

        assert_eq!(conditions.len(), 0);
        assert_eq!(binds.len(), 0);
    }
}
