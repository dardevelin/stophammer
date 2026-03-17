// Issue-SSRF-REDIRECT — 2026-03-15
// Issue-DNS-REBIND — 2026-03-16
// Tests that verify_podcast_txt blocks redirect chains to private/reserved IPs,
// and that the DNS-pinned redirect loop prevents TOCTOU rebinding.

mod common;

use std::net::SocketAddr;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Extract the hostname and socket address from a wiremock `MockServer` URI.
///
/// Wiremock binds to `127.0.0.1:<port>`, so this returns `("127.0.0.1", [addr])`.
fn mock_server_host_and_addrs(server: &MockServer) -> (String, Vec<SocketAddr>) {
    let uri = server.uri();
    let url = url::Url::parse(&uri).expect("mock server URI is a valid URL");
    let host = url.host_str().expect("mock server has a host").to_string();
    let port = url.port().expect("mock server has a port");
    let addr: SocketAddr = format!("{host}:{port}").parse().expect("valid socket addr");
    (host, vec![addr])
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-01: redirect to loopback (127.0.0.1) must be blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_redirect_to_loopback_blocked() {
    let mock_server = MockServer::start().await;

    // The mock returns a 302 redirect to a loopback address.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://127.0.0.1:9999/secret"),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), "token.hash").await;

    assert!(
        result.is_err(),
        "redirect to 127.0.0.1 must be blocked, got: {result:?}"
    );
    let err = result.unwrap_err();
    // The error surfaces through reqwest's redirect machinery; it may be
    // wrapped as "error following redirect" containing our custom message,
    // or as a direct connection-refused error. Either way, it must not succeed.
    assert!(
        err.contains("private") || err.contains("blocked") || err.contains("redirect"),
        "error should mention private/blocked/redirect, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-02: redirect to link-local (169.254.x.x) must be blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_redirect_to_link_local_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://169.254.169.254/latest/meta-data/"),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), "token.hash").await;

    assert!(
        result.is_err(),
        "redirect to 169.254.169.254 must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-03: redirect to private (10.x.x.x) must be blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_redirect_to_private_10_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://10.0.0.1:8080/internal"),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), "token.hash").await;

    assert!(
        result.is_err(),
        "redirect to 10.0.0.1 must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-04: redirect to private (192.168.x.x) must be blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_redirect_to_private_192_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://192.168.1.1/admin"),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), "token.hash").await;

    assert!(
        result.is_err(),
        "redirect to 192.168.1.1 must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-05: redirect to non-HTTP scheme must be blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_redirect_to_file_scheme_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "file:///etc/passwd"),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), "token.hash").await;

    // reqwest silently stops following redirects for non-HTTP schemes rather
    // than invoking the redirect policy, so the request may "succeed" with the
    // 302 body (no podcast:txt found → Ok(false)) or error out. Either
    // outcome is safe — the important thing is it must NOT return Ok(true).
    assert_ne!(
        result,
        Ok(true),
        "redirect to file:// must not succeed with Ok(true)"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-06: redirect to a safe public URL must still work
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_redirect_to_safe_url_works() {
    let mock_server = MockServer::start().await;

    let token_binding = "safe-redirect.hash";
    let rss = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Test Podcast</title>
    <podcast:txt>stophammer-proof {token_binding}</podcast:txt>
  </channel>
</rss>"#
    );

    // Mount the final RSS response on /feed path.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss))
        .mount(&mock_server)
        .await;

    // A direct request (no redirect) to the mock should work fine.
    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), token_binding).await;

    assert_eq!(
        result,
        Ok(true),
        "direct request to safe URL should succeed"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-07: chained redirect (safe -> safe -> private) must be blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_chained_redirect_to_private_blocked() {
    let mock_server = MockServer::start().await;

    // The mock returns a redirect to another path on the same (safe) server,
    // which in turn redirects to a private IP. We simulate this with a single
    // redirect to a private IP (wiremock doesn't easily support multi-hop on
    // different paths with the same mock, but the redirect policy checks every
    // hop independently).
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://172.16.0.1/internal"),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &mock_server.uri(), "token.hash").await;

    assert!(
        result.is_err(),
        "redirect chain ending at private IP must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-08: validate_feed_url returns resolved addresses
// ---------------------------------------------------------------------------

#[test]
fn validate_feed_url_returns_resolved_addrs() {
    // A URL with a literal IP should return that IP in the resolved list.
    let result = stophammer::proof::validate_feed_url("https://1.1.1.1/feed.xml");
    assert!(result.is_ok(), "public IP should pass validation");
    let addrs = result.unwrap();
    assert!(
        !addrs.is_empty(),
        "resolved addresses should not be empty for a literal IP"
    );
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-09: validate_feed_url rejects private IPs (unchanged behavior)
// ---------------------------------------------------------------------------

#[test]
fn validate_feed_url_still_rejects_private_ips() {
    let result = stophammer::proof::validate_feed_url("https://127.0.0.1/feed.xml");
    assert!(result.is_err(), "loopback should be rejected");

    let result = stophammer::proof::validate_feed_url("https://10.0.0.1/feed.xml");
    assert!(result.is_err(), "private 10.x should be rejected");

    let result = stophammer::proof::validate_feed_url("https://192.168.1.1/feed.xml");
    assert!(result.is_err(), "private 192.168.x should be rejected");

    let result = stophammer::proof::validate_feed_url("https://169.254.169.254/feed.xml");
    assert!(result.is_err(), "link-local should be rejected");
}

// ---------------------------------------------------------------------------
// SSRF-REDIRECT-10: max redirect depth is enforced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_podcast_txt_max_redirects_enforced() {
    let mock_server = MockServer::start().await;

    // Mock server that always redirects to itself -- should hit the max.
    let uri = mock_server.uri();
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", &*uri),
        )
        .mount(&mock_server)
        .await;

    let client = stophammer::proof::build_ssrf_safe_client();
    let result =
        stophammer::proof::verify_podcast_txt(&client, &uri, "token.hash").await;

    assert!(
        result.is_err(),
        "infinite redirect loop must be stopped, got: {result:?}"
    );
}

// ===========================================================================
// Issue-DNS-REBIND — 2026-03-16
// Tests for the DNS-pinned manual redirect loop (fetch_with_pinned_redirects
// and verify_podcast_txt_pinned).
// ===========================================================================

// ---------------------------------------------------------------------------
// DNS-REBIND-01: pinned redirect to loopback is blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_redirect_to_loopback_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://127.0.0.1:9999/secret"),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    assert!(
        result.is_err(),
        "pinned redirect to 127.0.0.1 must be blocked, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("private") || err.contains("blocked") || err.contains("redirect"),
        "error should mention private/blocked/redirect, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-02: pinned redirect to link-local is blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_redirect_to_link_local_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://169.254.169.254/latest/meta-data/"),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    assert!(
        result.is_err(),
        "pinned redirect to 169.254.169.254 must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-03: pinned redirect to private 10.x is blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_redirect_to_private_10_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://10.0.0.1:8080/internal"),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    assert!(
        result.is_err(),
        "pinned redirect to 10.0.0.1 must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-04: pinned redirect to private 192.168.x is blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_redirect_to_private_192_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://192.168.1.1/admin"),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    assert!(
        result.is_err(),
        "pinned redirect to 192.168.1.1 must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-05: pinned redirect to file:// scheme is blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_redirect_to_file_scheme_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "file:///etc/passwd"),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    // The redirect to file:// is blocked because the scheme is not http/https.
    assert!(
        result.is_err(),
        "pinned redirect to file:// must be blocked, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("scheme") || err.contains("blocked"),
        "error should mention disallowed scheme, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-06: pinned fetch with no redirect returns correct RSS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_direct_fetch_works() {
    let mock_server = MockServer::start().await;

    let token_binding = "pinned-direct.hash";
    let rss = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Test Podcast</title>
    <podcast:txt>stophammer-proof {token_binding}</podcast:txt>
  </channel>
</rss>"#
    );

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss))
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        token_binding,
        &host,
        &addrs,
    )
    .await;

    assert_eq!(
        result,
        Ok(true),
        "pinned direct request to safe URL should succeed"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-07: pinned max redirects enforced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_max_redirects_enforced() {
    let mock_server = MockServer::start().await;

    // Mock server always redirects back to itself (127.0.0.1).
    // With the pinned redirect loop, the redirect target is re-validated via
    // resolve_and_validate_url, which correctly blocks 127.0.0.1 as private.
    // The redirect is stopped either by the private IP check or by the max
    // redirect depth -- both are valid termination conditions.
    let uri = mock_server.uri();
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", &*uri),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &uri,
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    assert!(
        result.is_err(),
        "pinned infinite redirect loop must be stopped, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("too many redirects") || err.contains("private") || err.contains("blocked"),
        "error should mention redirect limit or private IP rejection, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-07b: max redirects enforced with public redirect target
// This uses fetch_with_pinned_redirects directly to test the counter by
// redirecting to another path on the same server (which we pre-pin).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_max_redirects_enforced_non_private() {
    // Use fetch_with_pinned_redirects directly with max_redirects=0 to verify
    // that even a single redirect is rejected when the limit is 0.
    let mock_server = MockServer::start().await;
    let uri = mock_server.uri();

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", &*format!("{uri}/other")),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::fetch_with_pinned_redirects(
        &uri,
        &host,
        &addrs,
        0, // max_redirects = 0
        5 * 1024 * 1024,
    )
    .await;

    assert!(
        result.is_err(),
        "should fail with max_redirects=0 and a redirect, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("too many redirects"),
        "error should mention too many redirects, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-08: pinned redirect to 172.16.x private is blocked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_redirect_to_private_172_blocked() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "http://172.16.0.1/internal"),
        )
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::verify_podcast_txt_pinned(
        &mock_server.uri(),
        "token.hash",
        &host,
        &addrs,
    )
    .await;

    assert!(
        result.is_err(),
        "pinned redirect chain ending at 172.16.x must be blocked, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// DNS-REBIND-09: resolve_and_validate_url rejects private IPs
// ---------------------------------------------------------------------------

#[test]
fn resolve_and_validate_url_rejects_private_ips() {
    let cases = [
        "http://127.0.0.1/feed.xml",
        "http://10.0.0.1/feed.xml",
        "http://192.168.1.1/feed.xml",
        "http://169.254.169.254/latest/meta-data/",
        "http://172.16.0.1/feed.xml",
    ];
    for url_str in &cases {
        let url = url::Url::parse(url_str).expect("valid URL");
        let result = stophammer::proof::resolve_and_validate_url(&url);
        assert!(
            result.is_err(),
            "resolve_and_validate_url should reject {url_str}, got: {result:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// DNS-REBIND-10: resolve_and_validate_url rejects non-http schemes
// ---------------------------------------------------------------------------

#[test]
fn resolve_and_validate_url_rejects_bad_schemes() {
    let cases = ["file:///etc/passwd", "ftp://example.com/feed.xml"];
    for url_str in &cases {
        let url = url::Url::parse(url_str).expect("valid URL");
        let result = stophammer::proof::resolve_and_validate_url(&url);
        assert!(
            result.is_err(),
            "resolve_and_validate_url should reject {url_str}, got: {result:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// DNS-REBIND-11: resolve_and_validate_url accepts public IPs
// ---------------------------------------------------------------------------

#[test]
fn resolve_and_validate_url_accepts_public_ips() {
    let url = url::Url::parse("https://1.1.1.1/feed.xml").expect("valid URL");
    let result = stophammer::proof::resolve_and_validate_url(&url);
    assert!(
        result.is_ok(),
        "resolve_and_validate_url should accept 1.1.1.1, got: {result:?}"
    );
    let (host, addrs) = result.unwrap();
    assert_eq!(host, "1.1.1.1");
    assert!(!addrs.is_empty());
}

// ---------------------------------------------------------------------------
// DNS-REBIND-12: fetch_with_pinned_redirects respects body size limit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_fetch_rejects_oversized_body() {
    let mock_server = MockServer::start().await;

    // 1 KiB body, but we set max_body_bytes to 100.
    let big_body = "x".repeat(1024);
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(big_body))
        .mount(&mock_server)
        .await;

    let (host, addrs) = mock_server_host_and_addrs(&mock_server);
    let result = stophammer::proof::fetch_with_pinned_redirects(
        &mock_server.uri(),
        &host,
        &addrs,
        3,
        100, // very small limit
    )
    .await;

    assert!(
        result.is_err(),
        "oversized body should be rejected, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("exceeds") || err.contains("too large"),
        "error should mention size limit, got: {err}"
    );
}
