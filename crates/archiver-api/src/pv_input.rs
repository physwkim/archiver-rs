//! Bulk PV input parsing utilities.
//!
//! Parses PV lists from POST bodies: JSON arrays or newline-delimited text.
//! Compatible with Java EPICS Archiver Appliance bulk endpoints.

use std::collections::HashSet;

use axum::body::Bytes;
use axum::extract::FromRequest;
use axum::http::{header, Request};

use crate::errors::ApiError;

/// Parse a PV list from a string body.
///
/// Tries JSON array first (`["PV:A", "PV:B"]`), falls back to newline-delimited.
/// Trims whitespace, removes empty lines, normalizes each name (strips
/// `ca://`/`pva://` prefixes and trailing `.VAL` per Java archiver
/// convention), and deduplicates (preserving order). Normalizing before
/// dedup means `PV:A` and `PV:A.VAL` collapse to one entry.
pub fn parse_pv_list(body: &str) -> Vec<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    // Try JSON array first.
    let raw: Vec<String> = if let Ok(arr) = serde_json::from_str::<Vec<String>>(trimmed) {
        arr
    } else {
        // Fallback: newline-delimited.
        trimmed.lines().map(|l| l.trim().to_string()).collect()
    };

    // Normalize + deduplicate while preserving order.
    let mut seen = HashSet::new();
    raw.into_iter()
        .map(|s| archiver_core::registry::normalize_pv_name(s.trim()).to_string())
        .filter(|s| !s.is_empty() && seen.insert(s.clone()))
        .collect()
}

/// Axum extractor that parses a bulk PV list from the request body.
///
/// - `application/json` content type → JSON array
/// - `text/plain` or other → newline-delimited
pub struct PvListInput(pub Vec<String>);

impl<S> FromRequest<S> for PvListInput
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request<axum::body::Body>, state: &S) -> Result<Self, Self::Rejection> {
        let content_type = req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_lowercase();

        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;

        let body = String::from_utf8(bytes.to_vec())
            .map_err(|_| ApiError::BadRequest("Invalid UTF-8 body".to_string()))?;

        let pvs = if content_type.contains("application/json") {
            let raw: Vec<String> = serde_json::from_str(&body)
                .map_err(|e| ApiError::BadRequest(format!("Invalid JSON array: {e}")))?;
            // Same normalize+dedup as the newline-delimited path so the
            // wire format the caller picks doesn't change behavior.
            let mut seen = HashSet::new();
            raw.into_iter()
                .map(|s| archiver_core::registry::normalize_pv_name(s.trim()).to_string())
                .filter(|s| !s.is_empty() && seen.insert(s.clone()))
                .collect()
        } else {
            parse_pv_list(&body)
        };

        Ok(PvListInput(pvs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_array() {
        let input = r#"["PV:A", "PV:B", "PV:C"]"#;
        let result = parse_pv_list(input);
        assert_eq!(result, vec!["PV:A", "PV:B", "PV:C"]);
    }

    #[test]
    fn test_newline_delimited() {
        let input = "PV:A\nPV:B\nPV:C";
        let result = parse_pv_list(input);
        assert_eq!(result, vec!["PV:A", "PV:B", "PV:C"]);
    }

    #[test]
    fn test_whitespace_and_empty_lines() {
        let input = "  PV:A  \n\n  PV:B  \n  \n  PV:C  \n";
        let result = parse_pv_list(input);
        assert_eq!(result, vec!["PV:A", "PV:B", "PV:C"]);
    }

    #[test]
    fn test_deduplication() {
        let input = "PV:A\nPV:B\nPV:A\nPV:C\nPV:B";
        let result = parse_pv_list(input);
        assert_eq!(result, vec!["PV:A", "PV:B", "PV:C"]);
    }

    #[test]
    fn test_json_deduplication() {
        let input = r#"["PV:A", "PV:B", "PV:A"]"#;
        let result = parse_pv_list(input);
        assert_eq!(result, vec!["PV:A", "PV:B"]);
    }

    #[test]
    fn test_empty_input() {
        assert!(parse_pv_list("").is_empty());
        assert!(parse_pv_list("   ").is_empty());
        assert!(parse_pv_list("\n\n").is_empty());
    }

    #[test]
    fn test_single_pv() {
        let result = parse_pv_list("PV:Single");
        assert_eq!(result, vec!["PV:Single"]);
    }

    #[test]
    fn test_json_with_whitespace() {
        let input = r#"  ["PV:A", " PV:B "]  "#;
        let result = parse_pv_list(input);
        assert_eq!(result, vec!["PV:A", "PV:B"]);
    }
}
