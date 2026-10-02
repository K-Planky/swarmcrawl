use std::{
    net::TcpListener,
    process::{Command, Output},
};

fn swarmcrawl(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_swarmcrawl"));
    command
        .args(args)
        .env_remove("SWARMCRAWL_REDIS_URL")
        .env_remove("SWARMCRAWL_REDIS_TIMEOUT_SECS")
        .env_remove("SWARMCRAWL_FETCH_TIMEOUT_SECS")
        .env_remove("SWARMCRAWL_JOB_NAMESPACE");
    for (name, value) in env {
        command.env(name, value);
    }
    command.output().expect("run swarmcrawl binary")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("UTF-8 CLI output")
}

#[test]
fn help_and_version_work_without_redis() {
    let help = swarmcrawl(&["--help"], &[]);
    assert!(help.status.success());
    let help = text(&help.stdout);
    assert!(help.contains("Usage: swarmcrawl [OPTIONS] <COMMAND>"));
    assert!(help.contains("--redis-url"));
    assert!(help.contains("[env: SWARMCRAWL_REDIS_URL]"));
    assert!(help.contains("[env: SWARMCRAWL_REDIS_TIMEOUT_SECS]"));
    assert!(help.contains("[env: SWARMCRAWL_JOB_NAMESPACE]"));
    assert_eq!(help.matches("[env: ").count(), 3);
    assert!(help.contains("--namespace"));
    for name in ["check", "node", "submit", "status", "stats"] {
        assert!(help.contains(name));
        let output = swarmcrawl(&[name, "--help"], &[]);
        assert!(output.status.success());
        let subcommand_help = text(&output.stdout);
        assert!(subcommand_help.contains(&format!("Usage: swarmcrawl {name}")));
        assert!(subcommand_help.contains("--namespace"));
        for setting in [
            "SWARMCRAWL_REDIS_URL",
            "SWARMCRAWL_REDIS_TIMEOUT_SECS",
            "SWARMCRAWL_JOB_NAMESPACE",
        ] {
            assert!(subcommand_help.contains(&format!("[env: {setting}]")));
        }
        assert_eq!(
            subcommand_help.matches("[env: ").count(),
            if name == "node" { 4 } else { 3 }
        );
    }
    let status_help = swarmcrawl(&["status", "--help"], &[]);
    assert!(text(&status_help.stdout).contains("--follow"));
    assert!(text(&status_help.stdout).contains("500 ms"));
    let node_help = swarmcrawl(&["node", "--help"], &[]);
    assert!(node_help.status.success());
    assert!(text(&node_help.stdout).contains("--fetch-timeout-secs"));
    assert!(text(&node_help.stdout).contains("[env: SWARMCRAWL_FETCH_TIMEOUT_SECS]"));
    assert!(text(&node_help.stdout).contains("--namespace"));

    let version = swarmcrawl(&["--version"], &[]);
    assert!(version.status.success());
    assert_eq!(
        text(&version.stdout).trim(),
        format!("swarmcrawl {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn environment_settings_bound_node_connection_attempts() {
    // A silent loopback peer exercises the configured connection deadline without
    // depending on an external Redis service or allowing any HTTP retrieval.
    let peer = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("redis://{}/0", peer.local_addr().unwrap());
    let output = swarmcrawl(
        &["node"],
        &[
            ("SWARMCRAWL_REDIS_URL", endpoint.as_str()),
            ("SWARMCRAWL_REDIS_TIMEOUT_SECS", "1"),
            ("SWARMCRAWL_FETCH_TIMEOUT_SECS", "2"),
            ("SWARMCRAWL_JOB_NAMESPACE", "swarmcrawl:cli-smoke:v1"),
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = text(&output.stderr);
    assert!(error.contains("Redis connect timed out"), "{error}");
    assert!(!error.contains(&endpoint));
}

#[test]
fn invalid_redis_url_fails_without_disclosing_credentials() {
    let output = swarmcrawl(
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
    let output = swarmcrawl(
        &["--help"],
        &[(
            "SWARMCRAWL_REDIS_URL",
            "redis://user:test-secret@localhost/0",
        )],
    );
    assert!(output.status.success());
    assert!(!text(&output.stdout).contains("test-secret"));
    for args in [
        vec!["--help"],
        vec!["submit", "--help"],
        vec!["status", "--help"],
        vec!["stats", "--help"],
    ] {
        let output = swarmcrawl(
            &args,
            &[("SWARMCRAWL_JOB_NAMESPACE", "fixture-sensitive-namespace")],
        );
        assert!(output.status.success());
        assert!(!text(&output.stdout).contains("fixture-sensitive-namespace"));
    }
}

#[test]
fn flags_override_environment_and_environment_overrides_defaults() {
    // Invalid inputs avoid network access; different errors show which source
    // was chosen without mutating this test process's environment.
    let output = swarmcrawl(&["check"], &[("SWARMCRAWL_REDIS_URL", "not-a-url")]);
    assert!(text(&output.stderr).contains("invalid Redis URL"));

    let output = swarmcrawl(
        &["--redis-url", "not-a-url", "check"],
        &[("SWARMCRAWL_REDIS_URL", "redis://127.0.0.1:6379/0")],
    );
    assert!(text(&output.stderr).contains("invalid Redis URL"));

    for args in [
        ["check", "--redis-timeout-secs", "5"],
        ["--redis-timeout-secs", "5", "check"],
    ] {
        let output = swarmcrawl(
            &args,
            &[
                ("SWARMCRAWL_REDIS_URL", "not-a-url"),
                ("SWARMCRAWL_REDIS_TIMEOUT_SECS", "0"),
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
        let output = swarmcrawl(&["--redis-timeout-secs", value, "check"], &[]);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(text(&output.stderr).contains("Redis timeout must be between 1 and 60 seconds"));
    }
    let output = swarmcrawl(&[], &[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(text(&output.stderr).contains("Usage:"));
    for args in [
        vec!["submit"],
        vec!["status"],
        vec!["status", "-f"],
        vec!["stats"],
    ] {
        let output = swarmcrawl(&args, &[]);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(text(&output.stderr).contains("Usage:"));
    }
}

#[test]
fn job_validation_precedes_network_access_and_never_echoes_inputs() {
    let endpoint = [("SWARMCRAWL_REDIS_URL", "redis://127.0.0.1:1/0")];
    let output = swarmcrawl(
        &[
            "submit",
            "https://example.org/valid/",
            "https://user:fixture-secret@example.org/?token=fixture-query",
        ],
        &endpoint,
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = text(&output.stderr);
    assert!(error.contains("submit input 2"));
    assert!(error.contains("no URLs submitted"));
    assert!(!error.contains("fixture-"));
    for command in ["status", "stats"] {
        let output = swarmcrawl(&[command, "fixture-sensitive-id"], &endpoint);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let error = text(&output.stderr);
        assert!(error.contains("invalid job ID"));
        assert!(!error.contains("fixture-sensitive-id"));
    }
    for args in [
        vec!["submit", "https://example.org/"],
        vec!["status", "1"],
        vec!["status", "-f", "1"],
        vec!["stats", "1"],
    ] {
        let output = swarmcrawl(&args, &[("SWARMCRAWL_JOB_NAMESPACE", "invalid namespace")]);
        assert_eq!(output.status.code(), Some(1));
        assert!(text(&output.stderr).contains("invalid job namespace"));
    }
}

#[test]
fn node_settings_are_validated_redacted_and_flags_override_environment() {
    for value in ["0", "301", "fixture-sensitive-value"] {
        let output = swarmcrawl(&["node", "--fetch-timeout-secs", value], &[]);
        assert_eq!(output.status.code(), Some(1));
        assert!(text(&output.stderr).contains("HTTP timeout must be between 1 and 300 seconds"));
        assert!(!text(&output.stderr).contains("fixture-sensitive-value"));
    }
    let output = swarmcrawl(
        &[
            "node",
            "--fetch-timeout-secs",
            "1",
            "--namespace",
            "has space",
        ],
        &[("SWARMCRAWL_FETCH_TIMEOUT_SECS", "fixture-sensitive-value")],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(text(&output.stderr).contains("invalid job namespace"));
    assert!(!text(&output.stderr).contains("fixture-sensitive-value"));

    let output = swarmcrawl(
        &["node", "--help"],
        &[
            ("SWARMCRAWL_FETCH_TIMEOUT_SECS", "fixture-sensitive-value"),
            ("SWARMCRAWL_JOB_NAMESPACE", "fixture-sensitive-namespace"),
        ],
    );
    assert!(output.status.success());
    assert!(!text(&output.stdout).contains("fixture-sensitive-"));
}
