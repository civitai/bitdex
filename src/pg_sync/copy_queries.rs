//! PostgreSQL COPY TO STDOUT queries and CSV chunk parser for bulk loading.
//!
//! Each table is streamed independently with no JOINs.
//!
//! This is significantly faster than JOIN-based loading because:
//! - No per-row deserialization through sqlx's type system
//! - No intermediate `Vec<Row>` allocation per batch
//! - Streaming backpressure: we process as fast as we can consume
//! - No JOINs: each table streams at sequential scan speed

use bytes::Bytes;
use futures_core::stream::BoxStream;
use sqlx::postgres::PgPoolCopyExt;
use sqlx::PgPool;

// ---------------------------------------------------------------------------
// COPY query functions — one per table, no JOINs
// ---------------------------------------------------------------------------

/// Stream Image table via COPY CSV (no JOINs).
///
/// Columns (13): id, url, nsfwLevel, hash, flags, type, userId, blockedFor,
///               scannedAtSecs, createdAtSecs, postId, width, height
pub async fn copy_images(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT id, url, "nsfwLevel", hash, flags, type::text,
                      "userId", "blockedFor",
                      extract(epoch from "scannedAt")::bigint,
                      extract(epoch from "createdAt")::bigint,
                      "postId",
                      width, height
               FROM "Image"
        ) TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream Post table via COPY CSV for enrichment.
///
/// Columns (4): id, publishedAtSecs, availability, modelVersionId
pub async fn copy_posts(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT id,
                      extract(epoch from "publishedAt")::bigint,
                      availability::text,
                      "modelVersionId"
               FROM "Post"
        ) TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream tags via COPY CSV (unordered).
///
/// Columns (2): tagId, imageId
pub async fn copy_tags(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "tagId", "imageId" FROM "TagsOnImageDetails" WHERE disabled = false) TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream tools via COPY CSV (unordered).
///
/// Columns (2): toolId, imageId
pub async fn copy_tools(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "toolId", "imageId" FROM "ImageTool") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream techniques via COPY CSV (unordered).
///
/// Columns (2): techniqueId, imageId
pub async fn copy_techniques(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "techniqueId", "imageId" FROM "ImageTechnique") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream ImageResourceNew via COPY CSV (no JOINs).
///
/// Columns (3): imageId, modelVersionId, detected
pub async fn copy_resources(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "imageId", "modelVersionId", detected FROM "ImageResourceNew") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream ModelVersion table via COPY CSV for enrichment.
///
/// Columns (3): id, baseModel, modelId
pub async fn copy_model_versions(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT id, "baseModel", "modelId" FROM "ModelVersion") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream CollectionItem via COPY CSV (accepted image collections only).
///
/// Columns (2): collectionId, imageId
pub async fn copy_collection_items(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT "collectionId", "imageId" FROM "CollectionItem" WHERE "imageId" IS NOT NULL AND status = 'ACCEPTED') TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
}

/// Stream Model table via COPY CSV for enrichment.
///
/// Columns (3): id, poi, type
pub async fn copy_models(
    pool: &PgPool,
) -> Result<BoxStream<'static, Result<Bytes, sqlx::Error>>, sqlx::Error> {
    pool.copy_out_raw(
        r#"COPY (SELECT id, poi, type::text FROM "Model") TO STDOUT WITH (FORMAT csv)"#,
    )
    .await
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
                        field.push(b'"');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    field.push(line[i]);
                    i += 1;
                }
            }
            fields.push(field);
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
        let lines1 = parser.feed(b"100,hello\n200,wor");
        assert_eq!(lines1.len(), 1);
        assert_eq!(lines1[0], b"100,hello");
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
        assert!(fields[1].is_empty());
        assert_eq!(fields[2], b"42");
        assert!(fields[3].is_empty());
        assert!(fields[4].is_empty());
    }

    #[test]
    fn test_parser_quoted_field_with_comma() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,\"hello,world\",42\n");
        assert_eq!(lines.len(), 1);
        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[1], b"hello,world");
    }

    #[test]
    fn test_parser_quoted_field_with_escaped_quote() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,\"say \"\"hi\"\"\",42\n");
        assert_eq!(lines.len(), 1);
        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields[1], b"say \"hi\"");
    }

    #[test]
    fn test_parser_quoted_field_with_newline() {
        let mut parser = CopyParser::new();
        let lines = parser.feed(b"100,\"line1\nline2\",42\n");
        assert_eq!(lines.len(), 1);
        let fields = split_csv_fields(&lines[0]);
        assert_eq!(fields[1], b"line1\nline2");
    }

    #[test]
    fn test_split_csv_simple() {
        let fields = split_csv_fields(b"a,b,c");
        assert_eq!(fields.len(), 3);
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
        let lines1 = parser.feed(b"1,a\n2,");
        assert_eq!(lines1.len(), 1);
        let lines2 = parser.feed(b"b\n3,c\n");
        assert_eq!(lines2.len(), 2);
    }
}
