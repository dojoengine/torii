use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use sqlx::sqlite::SqliteRow;
use torii_proto::{OrderDirection, Pagination, PaginationDirection};
use std::io::prelude::*;
use sqlx::Row;

use crate::error::{Error, QueryError};

pub(crate) fn build_cursor_conditions(
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
            return Err(Error::QueryError(QueryError::InvalidCursor(
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
                    format!("[{}.{}] {} ?", ob.model, ob.member, operator)
                } else {
                    let prev = (0..i)
                        .map(|j| {
                            let prev_ob = &pagination.order_by[j];
                            format!("[{}.{}] = ?", prev_ob.model, prev_ob.member)
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    format!("({} AND [{}.{}] {} ?)", prev, ob.model, ob.member, operator)
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

pub(crate) fn build_cursor_values(
    pagination: &Pagination,
    row: &SqliteRow,
) -> Result<Vec<String>, Error> {
    if pagination.order_by.is_empty() {
        Ok(vec![row.try_get("event_id")?])
    } else {
        let mut values: Vec<String> = pagination
            .order_by
            .iter()
            .map(|ob| row.try_get::<String, &str>(&format!("{}.{}", ob.model, ob.member)))
            .collect::<Result<Vec<_>, _>>()?;
        values.push(row.try_get("event_id")?);
        Ok(values)
    }
}

/// Compresses a string using Deflate and then encodes it using Base64 (no padding).
pub(crate) fn encode_cursor(value: &str) -> Result<String, Error> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(value.as_bytes()).map_err(|e| {
        Error::QueryError(QueryError::InvalidCursor(format!(
            "Cursor compression error: {}",
            e
        )))
    })?;
    let compressed_bytes = encoder.finish().map_err(|e| {
        Error::QueryError(QueryError::InvalidCursor(format!(
            "Cursor compression finish error: {}",
            e
        )))
    })?;

    Ok(BASE64_URL_SAFE_NO_PAD.encode(&compressed_bytes))
}

/// Decodes a Base64 (no padding) string and then decompresses it using Deflate.
pub(crate) fn decode_cursor(encoded_cursor: &str) -> Result<String, Error> {
    let compressed_cursor_bytes = BASE64_URL_SAFE_NO_PAD.decode(encoded_cursor).map_err(|e| {
        Error::QueryError(QueryError::InvalidCursor(format!(
            "Base64 decode error: {}",
            e
        )))
    })?;

    let mut decoder = DeflateDecoder::new(&compressed_cursor_bytes[..]);
    let mut decompressed_str = String::new();
    decoder.read_to_string(&mut decompressed_str).map_err(|e| {
        Error::QueryError(QueryError::InvalidCursor(format!(
            "Decompression error: {}",
            e
        )))
    })?;

    Ok(decompressed_str)
}
