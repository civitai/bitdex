//! Integration tests for query backpressure (concurrency limiting).
//!
//! These tests verify:
//! - Queries exceeding max_query_concurrency get 503 + Retry-After header
//! - The queries_rejected counter increments on rejection
//! - The in-flight guard correctly decrements counters on drop

#![cfg(feature = "server")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

/// Find a free TCP port by binding to port 0.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Send a raw HTTP POST and return the full response as a string.
fn http_post(addr: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, addr, body.len(), body
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    // Read until connection closes (Connection: close)
    let _ = stream.read_to_string(&mut response);
    response
}

/// Send a raw HTTP GET and return the full response as a string.
fn http_get(addr: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, addr
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response
}

#[test]
fn backpressure_rejects_with_503_and_retry_after() {
    // We need a tokio runtime for the server
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let port = free_port();
    let addr_str = format!("127.0.0.1:{}", port);
    let data_dir = tempfile::tempdir().unwrap();

    // Start server with max_query_concurrency=1
    let server_handle = rt.spawn(async move {
        let server = bitdex_v2::server::BitdexServer::new(data_dir.path().to_path_buf())
            .with_admin_token(Some("test-token".into()))
            .with_max_query_concurrency(1);
        let result: std::io::Result<()> = server
            .serve(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
            .await;
        result.unwrap();
    });

    // Wait for server to start accepting connections
    let mut ready = false;
    for _ in 0..50 {
        if TcpStream::connect(&addr_str).is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(ready, "Server failed to start within 5 seconds");

    // Create an index so we have something to query
    let create_body = serde_json::json!({
        "name": "test",
        "config": {
            "filter_fields": [],
            "sort_fields": []
        },
        "data_schema": {
            "id_field": "id",
            "fields": []
        }
    });
    let resp = http_post_with_auth(
        &addr_str,
        "/api/indexes",
        &create_body.to_string(),
        "test-token",
    );
    assert!(
        resp.contains("200 OK") || resp.contains("201") || resp.contains("\"name\":\"test\""),
        "Failed to create index: {}",
        resp
    );

    // Now fire concurrent queries. With max_concurrency=1, at least one should get 503.
    // We use threads to ensure actual concurrency.
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let barrier = Arc::clone(&barrier);
        let addr = addr_str.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let query_body = serde_json::json!({
                "query": {
                    "filter": [],
                    "sort_field": null,
                    "sort_direction": "Desc",
                    "limit": 10
                }
            });
            http_post(&addr, "/api/indexes/test/query", &query_body.to_string())
        }));
    }

    let responses: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let rejected_count = responses
        .iter()
        .filter(|r| r.contains("503 Service Unavailable"))
        .count();
    let has_retry_after = responses
        .iter()
        .any(|r| r.contains("503 Service Unavailable") && r.to_lowercase().contains("retry-after: 1"));

    // At least one should have been rejected (concurrency limit is 1, we sent 4)
    // Note: with very fast queries this is timing-dependent, so we check
    // the stats endpoint for the authoritative rejected count
    let stats_resp = http_get(&addr_str, "/api/indexes/test/stats");

    // Parse the rejected count from stats
    let stats_body = stats_resp
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("");
    let stats: serde_json::Value = serde_json::from_str(stats_body).unwrap_or(serde_json::json!({}));

    let queries_rejected = stats
        .get("queries_rejected")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // If any were rejected via HTTP, verify Retry-After header
    if rejected_count > 0 {
        assert!(
            has_retry_after,
            "503 response should include Retry-After header"
        );
    }

    // Verify in-flight is back to 0 (all queries completed)
    let in_flight = stats
        .get("queries_in_flight")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    assert_eq!(
        in_flight, 0,
        "in-flight counter should return to 0 after all queries complete"
    );

    // The peak should be at least 1
    let peak = stats
        .get("queries_in_flight_peak")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    assert!(peak >= 1, "peak should be at least 1, got {}", peak);

    // Print diagnostic info
    eprintln!(
        "Backpressure test: {} rejected via HTTP, {} via stats counter, peak={}",
        rejected_count, queries_rejected, peak
    );

    // At least one query should have been rejected, either observed in HTTP
    // responses or in the counter. With max_concurrency=1 and 4 concurrent
    // requests, the odds of all serializing perfectly are very low.
    assert!(
        rejected_count > 0 || queries_rejected > 0,
        "Expected at least one rejected query with max_concurrency=1 and 4 concurrent requests. \
         rejected_count={}, queries_rejected={}",
        rejected_count, queries_rejected
    );

    // Clean up: abort the server task
    server_handle.abort();
}

/// Send a raw HTTP POST with Authorization header.
fn http_post_with_auth(addr: &str, path: &str, body: &str, token: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, addr, token, body.len(), body
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response
}
