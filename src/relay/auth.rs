//! Per-route auth policy.
//!
//! - `None`              — no header check
//! - `Bearer`            — requires `Authorization: Bearer ${BITDEX_ADMIN_TOKEN}`
//! - `LoopbackOrBearer`  — peer_addr ∈ {127.0.0.1, ::1} bypasses; otherwise bearer required
//!
//! No `X-Forwarded-For`-based bypass anywhere — a missing XFF header is not a
//! safe trust boundary because direct pod-IP / NodePort access can omit it.

use std::net::{IpAddr, SocketAddr};

use axum::http::HeaderMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny(&'static str),
}

pub fn check(
    mode: crate::relay::config::AuthMode,
    headers: &HeaderMap,
    peer: SocketAddr,
    expected_token: Option<&str>,
) -> AuthDecision {
    use crate::relay::config::AuthMode;
    match mode {
        AuthMode::None => AuthDecision::Allow,
        AuthMode::Bearer => check_bearer(headers, expected_token),
        AuthMode::LoopbackOrBearer => {
            if is_loopback(peer.ip()) {
                AuthDecision::Allow
            } else {
                check_bearer(headers, expected_token)
            }
        }
    }
}

fn check_bearer(headers: &HeaderMap, expected: Option<&str>) -> AuthDecision {
    let Some(expected) = expected else {
        return AuthDecision::Deny("admin token not configured");
    };
    let Some(value) = headers.get("authorization") else {
        return AuthDecision::Deny("authorization header required");
    };
    let Ok(s) = value.to_str() else {
        return AuthDecision::Deny("authorization header malformed");
    };
    let Some(token) = s.strip_prefix("Bearer ") else {
        return AuthDecision::Deny("expected `Bearer <token>`");
    };
    // Constant-time-ish compare. The token is short and we don't expose
    // timing-side-channel attack surface in this code path (admin-only),
    // but a `==` with constant-time compare is a cheap correctness win.
    if subtle_eq(token.as_bytes(), expected.as_bytes()) {
        AuthDecision::Allow
    } else {
        AuthDecision::Deny("invalid token")
    }
}

fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}

fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::config::AuthMode;
    use axum::http::HeaderValue;

    fn ip(addr: &str) -> SocketAddr {
        addr.parse().unwrap()
    }

    fn hdr(key: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(key, HeaderValue::from_str(value).unwrap());
        h
    }

    #[test]
    fn none_always_allows() {
        let h = HeaderMap::new();
        assert_eq!(
            check(AuthMode::None, &h, ip("8.8.8.8:80"), None),
            AuthDecision::Allow
        );
    }

    #[test]
    fn bearer_requires_token_and_header() {
        let h = HeaderMap::new();
        assert!(matches!(
            check(AuthMode::Bearer, &h, ip("127.0.0.1:1"), Some("t")),
            AuthDecision::Deny(_)
        ));
        let h = hdr("authorization", "Bearer t");
        assert_eq!(
            check(AuthMode::Bearer, &h, ip("8.8.8.8:80"), Some("t")),
            AuthDecision::Allow
        );
    }

    #[test]
    fn bearer_rejects_wrong_token() {
        let h = hdr("authorization", "Bearer wrong");
        assert!(matches!(
            check(AuthMode::Bearer, &h, ip("8.8.8.8:80"), Some("t")),
            AuthDecision::Deny(_)
        ));
    }

    #[test]
    fn loopback_or_bearer_loopback_bypasses() {
        let h = HeaderMap::new();
        assert_eq!(
            check(AuthMode::LoopbackOrBearer, &h, ip("127.0.0.1:1"), Some("t")),
            AuthDecision::Allow
        );
        assert_eq!(
            check(AuthMode::LoopbackOrBearer, &h, ip("[::1]:1"), Some("t")),
            AuthDecision::Allow
        );
    }

    #[test]
    fn loopback_or_bearer_external_requires_token() {
        let h = HeaderMap::new();
        assert!(matches!(
            check(AuthMode::LoopbackOrBearer, &h, ip("8.8.8.8:80"), Some("t")),
            AuthDecision::Deny(_)
        ));
        let h = hdr("authorization", "Bearer t");
        assert_eq!(
            check(AuthMode::LoopbackOrBearer, &h, ip("8.8.8.8:80"), Some("t")),
            AuthDecision::Allow
        );
    }

    #[test]
    fn xff_header_does_not_bypass() {
        // A request with X-Forwarded-For but no Authorization is denied.
        // (No XFF code path here; this test documents the non-bypass.)
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_static("1.2.3.4"));
        assert!(matches!(
            check(AuthMode::Bearer, &h, ip("8.8.8.8:80"), Some("t")),
            AuthDecision::Deny(_)
        ));
    }
}
