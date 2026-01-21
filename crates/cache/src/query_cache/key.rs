use sha2::{Digest, Sha256};

/// Generate a cache key from SQL query and bind parameters.
///
/// The key format is: `torii:query:{table}:{hash}`
/// where hash is a SHA-256 hash of the SQL and bind parameters.
pub fn generate_cache_key(sql: &str, binds: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    for bind in binds {
        hasher.update(b"|"); // Separator
        hasher.update(bind.as_bytes());
    }
    let hash = hasher.finalize();

    // Extract table name for pattern invalidation
    let table = extract_table_name(sql).unwrap_or("unknown");
    format!("torii:query:{}:{:x}", table, hash)
}

/// Extract the primary table name from a SQL query.
///
/// Handles both bracketed `[table_name]` and plain table names.
fn extract_table_name(sql: &str) -> Option<&str> {
    let sql_upper = sql.to_uppercase();
    let from_idx = sql_upper.find("FROM ")?;
    let after_from = &sql[from_idx + 5..];
    let trimmed = after_from.trim_start();

    // Handle [bracketed] or plain table names
    if trimmed.starts_with('[') {
        let end = trimmed.find(']')?;
        Some(&trimmed[1..end])
    } else {
        let end = trimmed
            .find(|c: char| c.is_whitespace() || c == ',' || c == ')' || c == ';')
            .unwrap_or(trimmed.len());
        if end == 0 {
            None
        } else {
            Some(&trimmed[..end])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name_simple() {
        let sql = "SELECT * FROM entities WHERE id = ?";
        assert_eq!(extract_table_name(sql), Some("entities"));
    }

    #[test]
    fn test_extract_table_name_bracketed() {
        let sql = "SELECT * FROM [entity_model] WHERE id = ?";
        assert_eq!(extract_table_name(sql), Some("entity_model"));
    }

    #[test]
    fn test_extract_table_name_with_join() {
        let sql = "SELECT e.* FROM entities e JOIN models m ON e.id = m.entity_id";
        assert_eq!(extract_table_name(sql), Some("entities"));
    }

    #[test]
    fn test_extract_table_name_lowercase() {
        let sql = "select * from tokens where id = ?";
        assert_eq!(extract_table_name(sql), Some("tokens"));
    }

    #[test]
    fn test_generate_cache_key() {
        let sql = "SELECT * FROM entities WHERE id = ?";
        let binds = vec!["123".to_string()];
        let key = generate_cache_key(sql, &binds);

        assert!(key.starts_with("torii:query:entities:"));
        assert!(key.len() > 30); // Includes hash
    }

    #[test]
    fn test_generate_cache_key_different_binds() {
        let sql = "SELECT * FROM entities WHERE id = ?";
        let key1 = generate_cache_key(sql, &["123".to_string()]);
        let key2 = generate_cache_key(sql, &["456".to_string()]);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_generate_cache_key_same_query() {
        let sql = "SELECT * FROM entities WHERE id = ?";
        let binds = vec!["123".to_string()];
        let key1 = generate_cache_key(sql, &binds);
        let key2 = generate_cache_key(sql, &binds);

        assert_eq!(key1, key2);
    }
}
