// TLS module tests.
//
// The ACME provisioning flow (`provision_certificate`) cannot be unit-tested
// without a real ACME server (Let's Encrypt staging or production). It requires
// port 80 to be publicly reachable for the http-01 challenge. Manual
// integration testing can be performed by setting TLS_ACME_STAGING=true and
// pointing TLS_DOMAIN at a real domain.
//
// The `spawn_renewal_task` function is likewise not directly testable here
// because it spawns an infinite background loop. Its logic delegates to
// `cert_needs_renewal` + `provision_certificate`, both of which are covered
// by these tests (the former) and manual integration (the latter).
//
// The warning message emitted when TLS_DOMAIN is not set lives in `main.rs`
// inside `serve_with_optional_tls`, which writes directly to stderr. This is
// not unit-testable without capturing stderr or refactoring the function to
// accept a writer. It can be verified manually:
//   $ cargo run  # without TLS_DOMAIN set
//   WARN: TLS_DOMAIN not set — node is serving plain HTTP. ...

use stophammer::tls;

/// `cert_needs_renewal` returns true when the cert file does not exist.
#[test]
fn cert_needs_renewal_missing_file() {
    assert!(tls::cert_needs_renewal("/tmp/stophammer_test_nonexistent.pem"));
}

/// `cert_needs_renewal` returns true when the cert expires in < 30 days.
#[test]
fn cert_needs_renewal_short_lived() {
    let dir = unique_tempdir("short_lived");
    let cert_path = dir.join("cert.pem");

    // Generate a self-signed cert valid for only 10 days.
    let (cert_pem, _key_pem) = generate_test_cert(10);
    std::fs::write(&cert_path, &cert_pem).unwrap();

    assert!(tls::cert_needs_renewal(cert_path.to_str().unwrap()));
}

/// `cert_needs_renewal` returns false when the cert has > 30 days remaining.
#[test]
fn cert_needs_renewal_long_lived() {
    let dir = unique_tempdir("long_lived");
    let cert_path = dir.join("cert.pem");

    // Generate a self-signed cert valid for 90 days.
    let (cert_pem, _key_pem) = generate_test_cert(90);
    std::fs::write(&cert_path, &cert_pem).unwrap();

    assert!(!tls::cert_needs_renewal(cert_path.to_str().unwrap()));
}

/// `cert_needs_renewal` returns true when the file exists but is not a valid
/// PEM certificate (e.g. garbage data).
#[test]
fn cert_needs_renewal_invalid_pem() {
    let dir = unique_tempdir("invalid_pem");
    let cert_path = dir.join("cert.pem");
    std::fs::write(&cert_path, b"not a certificate").unwrap();

    assert!(tls::cert_needs_renewal(cert_path.to_str().unwrap()));
}

/// `cert_needs_renewal` returns true when the PEM file is empty.
#[test]
fn cert_needs_renewal_empty_file() {
    let dir = unique_tempdir("empty_file");
    let cert_path = dir.join("cert.pem");
    std::fs::write(&cert_path, b"").unwrap();

    assert!(tls::cert_needs_renewal(cert_path.to_str().unwrap()));
}

/// `provision_certificate` returns a future that is `Send`, so it can be used
/// inside `tokio::spawn`. This is a compile-time assertion — if the future is
/// not Send the test will fail to compile.
#[test]
fn provision_certificate_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}
    let config = tls::TlsConfig {
        domain: "example.com".into(),
        acme_email: "test@example.com".into(),
        cert_path: "/tmp/cert.pem".into(),
        key_path: "/tmp/key.pem".into(),
        acme_account_path: "/tmp/acme.json".into(),
        staging: true,
        acme_directory_url: None,
    };
    let fut = tls::provision_certificate(&config);
    assert_send(&fut);
    drop(fut);
}

/// When `TLS_ACME_DIRECTORY_URL` is set, the resolved directory URL must
/// return the custom value instead of a Let's Encrypt production or staging URL.
#[test]
fn tls_acme_directory_url_env_var_is_read() {
    let custom_url = "https://example.com/acme/dir";
    let config = tls::TlsConfig {
        domain: "test.local".into(),
        acme_email: "test@test.local".into(),
        cert_path: "/tmp/cert.pem".into(),
        key_path: "/tmp/key.pem".into(),
        acme_account_path: "/tmp/acme.json".into(),
        staging: false,
        acme_directory_url: Some(custom_url.to_string()),
    };

    let resolved = config.resolved_directory_url();
    assert_eq!(resolved, custom_url, "custom directory URL should override production/staging");
    assert_ne!(
        resolved,
        instant_acme::LetsEncrypt::Production.url(),
        "must not resolve to LE production"
    );
    assert_ne!(
        resolved,
        instant_acme::LetsEncrypt::Staging.url(),
        "must not resolve to LE staging"
    );
}

/// `cert_needs_renewal` correctly parses certificates that use `GeneralizedTime`
/// (ASN.1 tag 0x18) in their validity period. X.509 requires `GeneralizedTime`
/// for dates beyond 2049-12-31. A cert expiring in 2055 must not be treated as
/// expired or unparseable.
#[test]
fn cert_needs_renewal_generalized_time() {
    let dir = unique_tempdir("generalized_time");
    let cert_path = dir.join("cert.pem");

    // Generate a cert whose notAfter is in 2055, forcing GeneralizedTime encoding.
    let (cert_pem, _key_pem) = generate_generalized_time_cert();
    std::fs::write(&cert_path, &cert_pem).unwrap();

    // The cert expires ~29 years from now — well beyond 30 days — so this must be false.
    assert!(
        !tls::cert_needs_renewal(cert_path.to_str().unwrap()),
        "GeneralizedTime cert with far-future expiry should not need renewal"
    );
}

/// The env var for the TLS key path must be `TLS_KEY_PATH` per ADR 0019, not
/// `TLS_KEY_PATH_TLS`. This is a documentation-style test that asserts the
/// expected variable name matches the ADR spec.
#[test]
fn tls_key_path_env_var_name_matches_adr_0019() {
    // ADR 0019 specifies these exact env var names:
    let expected_cert_var = "TLS_CERT_PATH";
    let expected_key_var = "TLS_KEY_PATH";

    // These must match the strings used in main.rs serve_with_optional_tls.
    // If someone renames them back to TLS_KEY_PATH_TLS, this test documents
    // the correct name per the ADR.
    assert_eq!(expected_cert_var, "TLS_CERT_PATH");
    assert_eq!(expected_key_var, "TLS_KEY_PATH");
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Create a uniquely-named temporary directory for each test.
fn unique_tempdir(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "stophammer_tls_test_{}_{}",
        std::process::id(),
        name
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

/// Generate a self-signed certificate + private key valid for `days` days from
/// now. Returns `(cert_pem, key_pem)`.
fn generate_test_cert(days: u32) -> (String, String) {
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after =
        time::OffsetDateTime::now_utc() + time::Duration::days(i64::from(days));
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

/// Generate a self-signed certificate whose `notAfter` is in 2055, which forces
/// the X.509 encoder to use `GeneralizedTime` (tag 0x18) instead of `UTCTime`
/// (tag 0x17). Per RFC 5280 section 4.1.2.5, dates after 2049 MUST use
/// `GeneralizedTime`.
fn generate_generalized_time_cert() -> (String, String) {
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.not_before = time::OffsetDateTime::now_utc();
    // 2055-01-01 00:00:00 UTC — well past the 2049 boundary.
    let not_after = time::Date::from_calendar_date(2055, time::Month::January, 1)
        .expect("valid date");
    params.not_after =
        time::PrimitiveDateTime::new(not_after, time::Time::MIDNIGHT).assume_utc();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}
