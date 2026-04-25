//! Minimal placeholder substitution for relay payload templates.
//!
//! Supported tokens (V1, no engine):
//!   `{seq_id}`           u64 sequence ID
//!   `{ts_ms}`            u64 unix epoch ms
//!   `{body|json}`        request body re-encoded as compact JSON; on parse fail emits `null`
//!   `{path.<name>}`      named path parameter, JSON-string-escaped
//!   `{header.<name>}`    named header (lowercased name), JSON-string-escaped
//!   `{client_ip}`        XFF first hop or peer addr, JSON-string-escaped
//!
//! `{body}` (raw) was deliberately removed — see review fold-in.
//!
//! All non-`{body|json}` interpolations escape `"`, `\`, control chars so the
//! result is valid JSON when the template embeds them inside JSON strings.

use std::collections::HashMap;

pub struct TemplateContext<'a> {
    pub seq_id: u64,
    pub ts_ms: u64,
    pub body: &'a [u8],
    pub path_params: &'a HashMap<String, String>,
    pub headers: &'a HashMap<String, String>,
    pub client_ip: &'a str,
}

/// Render the template into out. Returns `Ok(())` on success.
/// Caller manages output buffer reuse.
pub fn render(template: &str, ctx: &TemplateContext, out: &mut String) -> RenderOutcome {
    out.clear();
    let bytes = template.as_bytes();
    let mut i = 0;
    let mut parse_err = false;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            // find matching '}'
            if let Some(close_off) = bytes[i + 1..].iter().position(|&c| c == b'}') {
                let token = &template[i + 1..i + 1 + close_off];
                if let Some(_) = expand(token, ctx, out, &mut parse_err) {
                    i += close_off + 2;
                    continue;
                }
            }
        }
        // Default: copy byte verbatim.
        out.push(bytes[i] as char);
        i += 1;
    }
    if parse_err {
        RenderOutcome::ParseErrorEmittedNull
    } else {
        RenderOutcome::Ok
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderOutcome {
    Ok,
    ParseErrorEmittedNull,
}

fn expand(token: &str, ctx: &TemplateContext, out: &mut String, parse_err: &mut bool) -> Option<()> {
    match token {
        "seq_id" => {
            out.push_str(&ctx.seq_id.to_string());
            Some(())
        }
        "ts_ms" => {
            out.push_str(&ctx.ts_ms.to_string());
            Some(())
        }
        "body|json" => {
            // Parse, then re-serialize compact. On parse failure emit `null`.
            match serde_json::from_slice::<serde_json::Value>(ctx.body) {
                Ok(v) => {
                    let s = serde_json::to_string(&v).unwrap_or_else(|_| "null".into());
                    out.push_str(&s);
                }
                Err(_) => {
                    out.push_str("null");
                    *parse_err = true;
                }
            }
            Some(())
        }
        "client_ip" => {
            push_json_escaped(out, ctx.client_ip);
            Some(())
        }
        _ if token.starts_with("path.") => {
            let name = &token[5..];
            let v = ctx.path_params.get(name).map(|s| s.as_str()).unwrap_or("");
            push_json_escaped(out, v);
            Some(())
        }
        _ if token.starts_with("header.") => {
            let name = &token[7..];
            let v = ctx.headers.get(name).map(|s| s.as_str()).unwrap_or("");
            push_json_escaped(out, v);
            Some(())
        }
        _ => None, // unknown token — leave the literal `{...}` in place
    }
}

/// Append `s` to `out` with JSON string-escaping (no surrounding quotes).
/// The template author is expected to put the `{token}` inside `"..."`; we
/// only escape the content so quotes/backslashes/control chars don't break
/// the JSON.
fn push_json_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(
        body: &'a [u8],
        paths: &'a HashMap<String, String>,
        headers: &'a HashMap<String, String>,
        ip: &'a str,
    ) -> TemplateContext<'a> {
        TemplateContext {
            seq_id: 42,
            ts_ms: 1_700_000_000_000,
            body,
            path_params: paths,
            headers,
            client_ip: ip,
        }
    }

    #[test]
    fn expands_basic_tokens() {
        let mut out = String::new();
        let p = HashMap::new();
        let h = HashMap::new();
        let c = ctx(b"{}", &p, &h, "127.0.0.1");
        let rc = render("{seq_id}-{ts_ms}", &c, &mut out);
        assert_eq!(out, "42-1700000000000");
        assert_eq!(rc, RenderOutcome::Ok);
    }

    #[test]
    fn expands_body_json() {
        let mut out = String::new();
        let p = HashMap::new();
        let h = HashMap::new();
        let c = ctx(br#"{"a":1}"#, &p, &h, "");
        render(r#"{"body":{body|json}}"#, &c, &mut out);
        // serde_json::Value re-serializes deterministically
        assert_eq!(out, r#"{"body":{"a":1}}"#);
    }

    #[test]
    fn body_json_parse_error_emits_null() {
        let mut out = String::new();
        let p = HashMap::new();
        let h = HashMap::new();
        let c = ctx(b"not-json", &p, &h, "");
        let rc = render("{body|json}", &c, &mut out);
        assert_eq!(out, "null");
        assert_eq!(rc, RenderOutcome::ParseErrorEmittedNull);
    }

    #[test]
    fn path_token_json_escapes_quote() {
        let mut out = String::new();
        let mut p = HashMap::new();
        p.insert("name".into(), r#"hello"world"#.into());
        let h = HashMap::new();
        let c = ctx(b"{}", &p, &h, "");
        render(r#""{path.name}""#, &c, &mut out);
        assert_eq!(out, r#""hello\"world""#);
    }

    #[test]
    fn header_token_handles_missing() {
        let mut out = String::new();
        let p = HashMap::new();
        let h = HashMap::new();
        let c = ctx(b"{}", &p, &h, "");
        render(r#"x-{header.x-missing}-y"#, &c, &mut out);
        assert_eq!(out, "x--y");
    }

    #[test]
    fn unknown_token_left_literal() {
        let mut out = String::new();
        let p = HashMap::new();
        let h = HashMap::new();
        let c = ctx(b"{}", &p, &h, "");
        render("{unknown_token}", &c, &mut out);
        assert_eq!(out, "{unknown_token}");
    }

    #[test]
    fn client_ip_escaped() {
        let mut out = String::new();
        let p = HashMap::new();
        let h = HashMap::new();
        let c = ctx(b"{}", &p, &h, "127.\n0.0.1");
        render(r#""{client_ip}""#, &c, &mut out);
        assert_eq!(out, r#""127.\n0.0.1""#);
    }
}
