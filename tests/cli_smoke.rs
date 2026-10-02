use std::process::{Command, Output};

fn crawl(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crawl"));
    command
        .args(args)
        .env_remove("CRAWL_REDIS_URL")
        .env_remove("CRAWL_REDIS_TIMEOUT_SECS")
        .env_remove("CRAWL_FETCH_TIMEOUT_SECS")
        .env_remove("CRAWL_JOB_NAMESPACE");
    for (name, value) in env {
        command.env(name, value);
    }
    command.output().expect("run crawl binary")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("UTF-8 CLI output")
}

#[test]
fn help_and_version_work_without_redis() {
    let help = crawl(&["--help"], &[]);
    assert!(help.status.success());
    let help = text(&help.stdout);
    assert!(help.contains("check"));
    assert!(help.contains("--redis-url"));
    assert!(help.contains("node"));
    let node_help = crawl(&["node", "--help"], &[]);
    assert!(node_help.status.success());
    assert!(text(&node_help.stdout).contains("--fetch-timeout-secs"));
    assert!(text(&node_help.stdout).contains("--namespace"));

    let version = crawl(&["--version"], &[]);
    assert!(version.status.success());
    assert_eq!(
        text(&version.stdout).trim(),
        format!("crawl {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn invalid_redis_url_fails_without_disclosing_credentials() {
    let output = crawl(
        &[
            "--redis-url",
            "redis://user:test-secret@localhost/bad-db",
            "check",
        ],
        &[],
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = text(&output.stderr);
    assert!(error.contains("invalid Redis URL"));
    assert!(!error.contains("test-secret"));
}

#[test]
fn help_hides_redis_environment_value() {
    let output = crawl(
        &["--help"],
        &[("CRAWL_REDIS_URL", "redis://user:test-secret@localhost/0")],
    );
    assert!(output.status.success());
    assert!(!text(&output.stdout).contains("test-secret"));
}

#[test]
fn flags_override_environment_and_environment_overrides_defaults() {
    // Invalid inputs avoid network access; different errors show which source
    // was chosen without mutating this test process's environment.
    let output = crawl(&["check"], &[("CRAWL_REDIS_URL", "not-a-url")]);
    assert!(text(&output.stderr).contains("invalid Redis URL"));

    let output = crawl(
        &["--redis-url", "not-a-url", "check"],
        &[("CRAWL_REDIS_URL", "redis://127.0.0.1:6379/0")],
    );
    assert!(text(&output.stderr).contains("invalid Redis URL"));

    for args in [
        ["check", "--redis-timeout-secs", "5"],
        ["--redis-timeout-secs", "5", "check"],
    ] {
        let output = crawl(
            &args,
            &[
                ("CRAWL_REDIS_URL", "not-a-url"),
                ("CRAWL_REDIS_TIMEOUT_SECS", "0"),
            ],
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(text(&output.stderr).contains("invalid Redis URL"));
        assert!(!text(&output.stderr).contains("Redis timeout must be"));
    }
}

#[test]
fn invalid_timeout_is_a_config_error_and_missing_command_is_a_usage_error() {
    for value in ["0", "61", "not-a-number"] {
        let output = crawl(&["--redis-timeout-secs", value, "check"], &[]);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(text(&output.stderr).contains("Redis timeout must be between 1 and 60 seconds"));
    }
    let output = crawl(&[], &[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(text(&output.stderr).contains("Usage:"));
}

#[test]
fn node_settings_are_validated_redacted_and_flags_override_environment() {
    for value in ["0", "301", "fixture-sensitive-value"] {
        let output = crawl(&["node", "--fetch-timeout-secs", value], &[]);
        assert_eq!(output.status.code(), Some(1));
        assert!(text(&output.stderr).contains("HTTP timeout must be between 1 and 300 seconds"));
        assert!(!text(&output.stderr).contains("fixture-sensitive-value"));
    }
    let output = crawl(
        &[
            "node",
            "--fetch-timeout-secs",
            "1",
            "--namespace",
            "has space",
        ],
        &[("CRAWL_FETCH_TIMEOUT_SECS", "fixture-sensitive-value")],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(text(&output.stderr).contains("invalid job namespace"));
    assert!(!text(&output.stderr).contains("fixture-sensitive-value"));

    let output = crawl(
        &["node", "--help"],
        &[
            ("CRAWL_FETCH_TIMEOUT_SECS", "fixture-sensitive-value"),
            ("CRAWL_JOB_NAMESPACE", "fixture-sensitive-namespace"),
        ],
    );
    assert!(output.status.success());
    assert!(!text(&output.stdout).contains("fixture-sensitive-"));
}
