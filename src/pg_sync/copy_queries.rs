//! PostgreSQL COPY TO STDOUT queries and CSV chunk parser for bulk loading.
//!
//! Each `copy_*` function issues a `COPY (...) TO STDOUT WITH (FORMAT csv)` via
//! `PgPoolCopyExt::copy_out_raw`, returning a stream of `Bytes` chunks. The
//! [`CopyParser`] reassembles these chunks into complete CSV lines, and the
//! `parse_*` functions convert raw CSV fields into typed rows.
//!
//! This is significantly faster than `fetch_all`-based loading because:
//! - No per-row deserialization through sqlx's type system
//! - No intermediate `Vec<Row>` allocation per batch
//! - Streaming backpressure: we process as fast as we can consume

use bytes::Bytes;
use futures_core::stream::BoxStream;
use sqlx::postgres::PgPoolCopyExt;
use sqlx::PgPool;

// ---------------------------------------------------------------------------
// COPY query functions
// ---------------------------------------------------------------------------

/// Stream Image+Post join via COPY CSV.
///
/// Columns: id, url, nsfwLevel, hash, flags, type, userId, blockedFor,
///          scannedAtSecs, createdAtSecs, publishedAtSecs, availability, modelVersionId, postId
pub async fn copy_images(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT i.id, i.url, i."nsfwLevel", i.hash, i.flags, i.type::text,
                      i."userId", i."blockedFor",
                      extract(epoch from i."scannedAt")::bigint as "scannedAtSecs",
                      extract(epoch from i."createdAt")::bigint as "createdAtSecs",
                      extract(epoch from p."publishedAt")::bigint as "publishedAtSecs",
                      p.availability::text, p."modelVersionId",
                      i."postId"
               FROM "Image" i
               LEFT JOIN "Post" p ON p.id = i."postId"
        ) TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream tags ordered by tagId for bitmap locality via COPY CSV.
///
/// Columns: tagId, imageId
pub async fn copy_tags(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "tagId", "imageId" FROM "TagsOnImageDetails" WHERE disabled = false ORDER BY "tagId", "imageId") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream tools via COPY CSV.
///
/// Columns: toolId, imageId
pub async fn copy_tools(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "toolId", "imageId" FROM "ImageTool" ORDER BY "toolId", "imageId") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream techniques via COPY CSV.
///
/// Columns: techniqueId, imageId
pub async fn copy_techniques(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "techniqueId", "imageId" FROM "ImageTechnique" ORDER BY "techniqueId", "imageId") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream resources with ModelVersion+Model join via COPY CSV.
///
/// Columns: imageId, modelVersionId, detected, baseModel, poi, type
pub async fn copy_resources(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT ir."imageId", ir."modelVersionId", ir.detected,
                      mv."baseModel", m.poi, m.type::text
               FROM "ImageResourceNew" ir
               JOIN "ModelVersion" mv ON ir."modelVersionId" = mv.id
               JOIN "Model" m ON mv."modelId" = m.id
               ORDER BY ir."imageId"
        ) TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// Lightweight image row parsed from COPY CSV stream.
/// Uses Image.flags bitfield instead of meta JSONB.
#[derive(Debug)]
pub struct CopyImageRow {
    pub id: i64,
    pub url: Option<String>,
    pub nsfw_level: i32,
    pub hash: Option<String>,
    pub flags: i16,
    pub image_type: String,
    pub user_id: i64,
    pub blocked_for: Option<String>,
    pub scanned_at_secs: Option<i64>,
    pub created_at_secs: Option<i64>,
    pub published_at_secs: Option<i64>,
    pub availability: String,
    pub posted_to_id: Option<i64>,
    pub post_id: Option<i64>,
}

impl CopyImageRow {
    /// hasMeta = hasPrompt(bit13) AND NOT hideMeta(bit2)
    #[inline]
    pub fn has_meta(&self) -> bool {
        (self.flags & (1 << 13)) != 0 && (self.flags & (1 << 2)) == 0
    }

    /// onSite = madeOnSite(bit14)
    #[inline]
    pub fn on_site(&self) -> bool {
        (self.flags & (1 << 14)) != 0
    }

    /// minor = bit3
    #[inline]
    pub fn minor(&self) -> bool {
        (self.flags & (1 << 3)) != 0
    }

    /// poi = bit4 (image-level; OR'd with resource_poi later)
    #[inline]
    pub fn poi(&self) -> bool {
        (self.flags & (1 << 4)) != 0
    }

    /// sortAt = max(published_at, scanned_at, created_at) in epoch seconds
    #[inline]
    pub fn sort_at_secs(&self) -> u64 {
        let vals = [
            self.published_at_secs.unwrap_or(0),
            self.scanned_at_secs.unwrap_or(0),
            self.created_at_secs.unwrap_or(0),
        ];
        vals.into_iter().max().unwrap_or(0) as u64
    }
}

/// Resource row from COPY CSV — one row per (imageId, modelVersionId) pair.
#[derive(Debug)]
pub struct CopyResourceRow {
    pub image_id: i64,
    pub model_version_id: i64,
    pub detected: bool,
    pub base_model: Option<String>,
    pub model_poi: bool,
    pub model_type: String,
}

// ---------------------------------------------------------------------------
// CSV chunk parser
// ---------------------------------------------------------------------------

/// Incremental CSV parser that buffers across `Bytes` chunk boundaries.
///
/// PostgreSQL's `COPY ... TO STDOUT WITH (FORMAT csv)` sends data in arbitrary
/// chunk sizes that may split CSV rows mid-line. This parser accumulates bytes
/// and yields only complete lines.
pub struct CopyParser {
    buffer: Vec<u8>,
}

impl CopyParser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024),
        }
    }

    /// Feed a chunk of bytes. Returns complete lines that can be parsed.
    /// Retains any incomplete trailing line in the internal buffer.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(chunk);

        let mut lines = Vec::new();
        let mut start = 0;
        let mut in_quote = false;

        let buf = &self.buffer;
        let len = buf.len();
        let mut i = 0;

        while i < len {
            let b = buf[i];
            if b == b'"' {
                in_quote = !in_quote;
            } else if b == b'\n' && !in_quote {
                // Complete line found (excluding the newline).
                lines.push(buf[start..i].to_vec());
                start = i + 1;
            }
            i += 1;
        }

        // Keep the incomplete trailing data for the next feed.
        if start == len {
            self.buffer.clear();
        } else if start > 0 {
            // Shift remaining bytes to the front.
            let remaining = self.buffer[start..].to_vec();
            self.buffer = remaining;
        }
        // If start == 0, the entire buffer is an incomplete line — keep as-is.

        lines
    }
}

// ---------------------------------------------------------------------------
// CSV field splitting
// ---------------------------------------------------------------------------

/// Split a CSV line into fields, handling quoted fields.
///
/// Rules (PostgreSQL CSV format):
/// - Fields separated by `,`
/// - Quoted fields start and end with `"`
/// - A literal `"` inside a quoted field is represented as `""`
/// - NULL is an empty unquoted field
fn split_csv_fields(line: &[u8]) -> Vec<Vec<u8>> {
    let mut fields = Vec::new();
    let mut i = 0;
    let len = line.len();

    while i <= len {
        if i == len {
            // Trailing empty field after final comma (only if line ends with comma).
            // Actually, we break below when we hit len. This handles the edge case
            // where we're past all content.
            fields.push(Vec::new());
            break;
        }

        if line[i] == b'"' {
            // Quoted field.
            let mut field = Vec::new();
            i += 1; // skip opening quote
            while i < len {
                if line[i] == b'"' {
                    if i + 1 < len && line[i + 1] == b'"' {
                        // Escaped quote.
                        field.push(b'"');
                        i += 2;
                    } else {
                        // Closing quote.
                        i += 1;
                        break;
                    }
                } else {
                    field.push(line[i]);
                    i += 1;
                }
            }
            fields.push(field);
            // Skip comma after quoted field.
            if i < len && line[i] == b',' {
                i += 1;
            }
        } else {
            // Unquoted field — scan until comma or end.
            let start = i;
            while i < len && line[i] != b',' {
                i += 1;
            }
            fields.push(line[start..i].to_vec());
            if i < len {
                i += 1; // skip comma
            } else {
                break;
            }
        }
    }

    fields
}

// ---------------------------------------------------------------------------
// Fast integer parsing
// ---------------------------------------------------------------------------

/// Parse bytes as i64 without going through str. Returns None on empty/invalid.
#[inline]
fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }

    let (negative, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };

    if start >= bytes.len() {
        return None;
    }

    let mut val: i64 = 0;
    for &b in &bytes[start..] {
        if b < b'0' || b > b'9' {
            return None;
        }
        val = val.wrapping_mul(10).wrapping_add((b - b'0') as i64);
    }

    if negative {
        Some(-val)
    } else {
        Some(val)
    }
}

/// Parse bytes as i16. Returns None on empty/invalid.
#[inline]
fn parse_i16_fast(bytes: &[u8]) -> Option<i16> {
    parse_i64_fast(bytes).map(|v| v as i16)
}

/// Parse bytes as i32. Returns None on empty/invalid.
#[inline]
fn parse_i32_fast(bytes: &[u8]) -> Option<i32> {
    parse_i64_fast(bytes).map(|v| v as i32)
}

/// Check if a field represents a PG CSV NULL (empty unquoted field).
#[inline]
fn is_null(field: &[u8]) -> bool {
    field.is_empty()
}

/// Parse an optional i64 — returns None for empty (NULL) fields.
#[inline]
fn parse_opt_i64(field: &[u8]) -> Option<i64> {
    if is_null(field) {
        None
    } else {
        parse_i64_fast(field)
    }
}

/// Parse an optional string — returns None for empty (NULL) fields.
#[inline]
fn parse_opt_string(field: &[u8]) -> Option<String> {
    if is_null(field) {
        None
    } else {
        Some(String::from_utf8_lossy(field).into_owned())
    }
}

/// Parse a PG boolean (`t`/`f`).
#[inline]
fn parse_bool(field: &[u8]) -> bool {
    !field.is_empty() && field[0] == b't'
}

// ---------------------------------------------------------------------------
// Row parse functions
// ---------------------------------------------------------------------------

/// Parse a CSV line into a [`CopyImageRow`].
///
/// Expected field order (14 fields):
///   id, url, nsfwLevel, hash, flags, type, userId, blockedFor,
///   scannedAtSecs, createdAtSecs, publishedAtSecs, availability, modelVersionId, postId
pub fn parse_image_row(line: &[u8]) -> Option<CopyImageRow> {
    let fields = split_csv_fields(line);
    if fields.len() < 14 {
        return None;
    }

    Some(CopyImageRow {
        id: parse_i64_fast(&fields[0])?,
        url: parse_opt_string(&fields[1]),
        nsfw_level: parse_i32_fast(&fields[2]).unwrap_or(0),
        hash: parse_opt_string(&fields[3]),
        flags: parse_i16_fast(&fields[4]).unwrap_or(0),
        image_type: String::from_utf8_lossy(&fields[5]).into_owned(),
        user_id: parse_i64_fast(&fields[6])?,
        blocked_for: parse_opt_string(&fields[7]),
        scanned_at_secs: parse_opt_i64(&fields[8]),
        created_at_secs: parse_opt_i64(&fields[9]),
        published_at_secs: parse_opt_i64(&fields[10]),
        availability: if is_null(&fields[11]) {
            String::new()
        } else {
            String::from_utf8_lossy(&fields[11]).into_owned()
        },
        posted_to_id: parse_opt_i64(&fields[12]),
        post_id: parse_opt_i64(&fields[13]),
    })
}

/// Parse a CSV line into a (tag_id, image_id) pair.
pub fn parse_tag_row(line: &[u8]) -> Option<(i64, i64)> {
    let fields = split_csv_fields(line);
    if fields.len() < 2 {
        return None;
    }
    Some((parse_i64_fast(&fields[0])?, parse_i64_fast(&fields[1])?))
}

/// Parse a CSV line into a (tool_id, image_id) pair.
pub fn parse_tool_row(line: &[u8]) -> Option<(i64, i64)> {
    let fields = split_csv_fields(line);
    if fields.len() < 2 {
        return None;
    }
    Some((parse_i64_fast(&fields[0])?, parse_i64_fast(&fields[1])?))
}

/// Parse a CSV line into a (technique_id, image_id) pair.
pub fn parse_technique_row(line: &[u8]) -> Option<(i64, i64)> {
    let fields = split_csv_fields(line);
    if fields.len() < 2 {
        return None;
    }
    Some((parse_i64_fast(&fields[0])?, parse_i64_fast(&fields[1])?))
}

/// Parse a CSV line into a [`CopyResourceRow`].
///
/// Expected field order (6 fields):
///   imageId, modelVersionId, detected, baseModel, poi, type
pub fn parse_resource_row(line: &[u8]) -> Option<CopyResourceRow> {
    let fields = split_csv_fields(line);
    if fields.len() < 6 {
        return None;
    }

    Some(CopyResourceRow {
        image_id: parse_i64_fast(&fields[0])?,
        model_version_id: parse_i64_fast(&fields[1])?,
        detected: parse_bool(&fields[2]),
        base_model: parse_opt_string(&fields[3]),
        model_poi: parse_bool(&fields[4]),
        model_type: String::from_utf8_lossy(&fields[5]).into_owned(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_basic_lines() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,hello,42\n200,world,99\n");
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], b"100,hello,42");
        assert_eq!(lines[1], b"200,world,99");
    }

    #[test]
    fn test_parser_chunk_boundary() {
        let mut parser = CopyParser::new();

        // First chunk: one complete line + partial second line.
        let lines1 = parser.feed(b"100,hello\n200,wor");
        assert_eq!(lines1.len(), 1);
        assert_eq!(lines1[0], b"100,hello");

        // Second chunk: rest of second line.
        let lines2 = parser.feed(b"ld\n");
        assert_eq!(lines2.len(), 1);
        assert_eq!(lines2[0], b"200,world");
    }

    #[test]
    fn test_parser_no_trailing_newline() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,hello\n200,world");
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], b"100,hello");

        // Flush with final newline.
        let lines2 = parser.feed(b"\n");
        assert_eq!(lines2.len(), 1);
        assert_eq!(lines2[0], b"200,world");
    }

    #[test]
    fn test_parser_empty_fields_null() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,,42,,\n");
        assert_eq!(lines.len(), 1);

        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0], b"100");
        assert!(fields[1].is_empty()); // NULL
        assert_eq!(fields[2], b"42");
        assert!(fields[3].is_empty()); // NULL
        assert!(fields[4].is_empty()); // NULL
    }

    #[test]
    fn test_parser_quoted_field_with_comma() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,\"hello,world\",42\n");
        assert_eq!(lines.len(), 1);

        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"100");
        assert_eq!(fields[1], b"hello,world");
        assert_eq!(fields[2], b"42");
    }

    #[test]
    fn test_parser_quoted_field_with_escaped_quote() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,\"say \"\"hi\"\"\",42\n");
        assert_eq!(lines.len(), 1);

        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"100");
        assert_eq!(fields[1], b"say \"hi\"");
        assert_eq!(fields[2], b"42");
    }

    #[test]
    fn test_parser_quoted_field_with_newline() {
        let mut parser = CopyParser::new();
        // A quoted field containing a newline should NOT split the line.
        let lines = parser.feed(b"100,\"line1\nline2\",42\n");
        assert_eq!(lines.len(), 1);

        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"100");
        assert_eq!(fields[1], b"line1\nline2");
        assert_eq!(fields[2], b"42");
    }

    #[test]
    fn test_parse_i64_fast() {
        assert_eq!(parse_i64_fast(b"12345"), Some(12345));
        assert_eq!(parse_i64_fast(b"-99"), Some(-99));
        assert_eq!(parse_i64_fast(b"0"), Some(0));
        assert_eq!(parse_i64_fast(b""), None);
        assert_eq!(parse_i64_fast(b"abc"), None);
        assert_eq!(parse_i64_fast(b"-"), None);
    }

    #[test]
    fn test_parse_image_row() {
        let line = b"12345,https://example.com/img.jpg,8,abc123,8196,image,9999,,1700000000,1699000000,1700500000,Public,42,777";
        let row = parse_image_row(line).expect("should parse");
        assert_eq!(row.id, 12345);
        assert_eq!(row.url.as_deref(), Some("https://example.com/img.jpg"));
        assert_eq!(row.nsfw_level, 8);
        assert_eq!(row.hash.as_deref(), Some("abc123"));
        assert_eq!(row.flags, 8196); // bit2 + bit13 = 4 + 8192
        assert_eq!(row.image_type, "image");
        assert_eq!(row.user_id, 9999);
        assert!(row.blocked_for.is_none());
        assert_eq!(row.scanned_at_secs, Some(1700000000));
        assert_eq!(row.created_at_secs, Some(1699000000));
        assert_eq!(row.published_at_secs, Some(1700500000));
        assert_eq!(row.availability, "Public");
        assert_eq!(row.posted_to_id, Some(42));
        assert_eq!(row.post_id, Some(777));
    }

    #[test]
    fn test_parse_image_row_nulls() {
        let line = b"12345,,,,,image,9999,,,,,Public,,";
        let row = parse_image_row(line).expect("should parse");
        assert_eq!(row.id, 12345);
        assert!(row.url.is_none());
        assert_eq!(row.nsfw_level, 0);
        assert!(row.hash.is_none());
        assert_eq!(row.flags, 0);
        assert!(row.scanned_at_secs.is_none());
        assert!(row.created_at_secs.is_none());
        assert!(row.published_at_secs.is_none());
        assert!(row.posted_to_id.is_none());
        assert!(row.post_id.is_none());
    }

    #[test]
    fn test_flags_has_meta() {
        // bit13 = 8192, bit2 = 4
        let row = CopyImageRow {
            id: 1, url: None, nsfw_level: 0, hash: None,
            flags: (1 << 13), // hasPrompt=true, hideMeta=false
            image_type: String::new(), user_id: 1, blocked_for: None,
            scanned_at_secs: None, created_at_secs: None, published_at_secs: None,
            availability: String::new(), posted_to_id: None, post_id: None,
        };
        assert!(row.has_meta());

        // hasPrompt=true, hideMeta=true → has_meta = false
        let row2 = CopyImageRow { flags: (1 << 13) | (1 << 2), ..row };
        assert!(!row2.has_meta());

        // hasPrompt=false → has_meta = false
        let row3 = CopyImageRow { flags: 0, ..row2 };
        assert!(!row3.has_meta());
    }

    #[test]
    fn test_flags_on_site() {
        let row = CopyImageRow {
            id: 1, url: None, nsfw_level: 0, hash: None,
            flags: (1 << 14),
            image_type: String::new(), user_id: 1, blocked_for: None,
            scanned_at_secs: None, created_at_secs: None, published_at_secs: None,
            availability: String::new(), posted_to_id: None, post_id: None,
        };
        assert!(row.on_site());

        let row2 = CopyImageRow { flags: 0, ..row };
        assert!(!row2.on_site());
    }

    #[test]
    fn test_flags_minor_and_poi() {
        let base = CopyImageRow {
            id: 1, url: None, nsfw_level: 0, hash: None, flags: 0,
            image_type: String::new(), user_id: 1, blocked_for: None,
            scanned_at_secs: None, created_at_secs: None, published_at_secs: None,
            availability: String::new(), posted_to_id: None, post_id: None,
        };

        assert!(!base.minor());
        assert!(!base.poi());

        let with_minor = CopyImageRow { flags: (1 << 3), ..base };
        assert!(with_minor.minor());
        assert!(!with_minor.poi());

        let with_poi = CopyImageRow { flags: (1 << 4), ..with_minor };
        assert!(with_poi.poi());
    }

    #[test]
    fn test_sort_at_secs() {
        let row = CopyImageRow {
            id: 1, url: None, nsfw_level: 0, hash: None, flags: 0,
            image_type: String::new(), user_id: 1, blocked_for: None,
            scanned_at_secs: Some(100),
            created_at_secs: Some(200),
            published_at_secs: Some(150),
            availability: String::new(), posted_to_id: None, post_id: None,
        };
        assert_eq!(row.sort_at_secs(), 200);

        let row2 = CopyImageRow {
            scanned_at_secs: None, created_at_secs: None, published_at_secs: None,
            ..row
        };
        assert_eq!(row2.sort_at_secs(), 0);
    }

    #[test]
    fn test_parse_tag_row() {
        assert_eq!(parse_tag_row(b"42,12345"), Some((42, 12345)));
        assert_eq!(parse_tag_row(b""), None);
        assert_eq!(parse_tag_row(b"42"), None);
    }

    #[test]
    fn test_parse_tool_row() {
        assert_eq!(parse_tool_row(b"7,99999"), Some((7, 99999)));
    }

    #[test]
    fn test_parse_technique_row() {
        assert_eq!(parse_technique_row(b"3,88888"), Some((3, 88888)));
    }

    #[test]
    fn test_parse_resource_row() {
        let line = b"12345,678,t,SD 1.5,f,Checkpoint";
        let row = parse_resource_row(line).expect("should parse");
        assert_eq!(row.image_id, 12345);
        assert_eq!(row.model_version_id, 678);
        assert!(row.detected);
        assert_eq!(row.base_model.as_deref(), Some("SD 1.5"));
        assert!(!row.model_poi);
        assert_eq!(row.model_type, "Checkpoint");
    }

    #[test]
    fn test_parse_resource_row_nulls() {
        let line = b"12345,678,f,,,LoRA";
        let row = parse_resource_row(line).expect("should parse");
        assert_eq!(row.image_id, 12345);
        assert!(!row.detected);
        assert!(row.base_model.is_none());
        assert!(!row.model_poi);
        assert_eq!(row.model_type, "LoRA");
    }

    #[test]
    fn test_split_csv_simple() {
        let fields = split_csv_fields(b"a,b,c");
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], b"a");
        assert_eq!(fields[1], b"b");
        assert_eq!(fields[2], b"c");
    }

    #[test]
    fn test_split_csv_trailing_comma() {
        let fields = split_csv_fields(b"a,b,");
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[2], b"");
    }

    #[test]
    fn test_multiple_chunks_interleaved() {
        let mut parser = CopyParser::new();

        // Simulate realistic streaming: multiple small chunks.
        let lines1 = parser.feed(b"1,a\n2,");
        assert_eq!(lines1.len(), 1);

        let lines2 = parser.feed(b"b\n3,c\n");
        assert_eq!(lines2.len(), 2);
        assert_eq!(lines2[0], b"2,b");
        assert_eq!(lines2[1], b"3,c");
    }
}
