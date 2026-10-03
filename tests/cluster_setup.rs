//! CLI-managed setup contracts. Real provisioning tests need Docker + GNU timeout.
#![cfg(unix)]

use std::{
    fs,
    io::Write,
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use serde_json::{Value, json};

struct Sandbox {
    root: tempfile::TempDir,
}

impl Sandbox {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
        }
    }
    fn config(&self) -> PathBuf {
        self.root.path().join("config")
    }
    fn run(&self, args: &[&str], input: Option<&[u8]>, env: &[(&str, &str)]) -> Output {
        let mut command = timeout_command();
        command
            .args(["180s", env!("CARGO_BIN_EXE_swarmcrawl")])
            .args(args)
            .env_remove("SWARMCRAWL_REDIS_URL")
            .env_remove("SWARMCRAWL_REDIS_TIMEOUT_SECS")
            .env_remove("SWARMCRAWL_JOB_NAMESPACE")
            .env_remove("SWARMCRAWL_FETCH_TIMEOUT_SECS")
            .env("SWARMCRAWL_CONFIG_DIR", self.config())
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().unwrap();
        if let Some(input) = input {
            child.stdin.take().unwrap().write_all(input).unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert_ne!(
            output.status.code(),
            Some(124),
            "CLI exceeded test deadline"
        );
        output
    }
    fn init(&self, port: &str, password: &[u8], env: &[(&str, &str)]) -> Output {
        self.run(
            &[
                "cluster",
                "init",
                "127.0.0.1",
                "--port",
                port,
                "--password-stdin",
            ],
            Some(password),
            env,
        )
    }
    fn saved(&self) -> Value {
        serde_json::from_slice(&fs::read(self.config().join("config.json")).unwrap()).unwrap()
    }
    fn save(&self, value: &Value) {
        fs::create_dir_all(self.config()).unwrap();
        fs::set_permissions(self.config(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = self.config().join("config.json");
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn fake_docker(&self, body: &str) -> String {
        let bin = self.root.path().join("bin");
        fs::create_dir_all(&bin).unwrap();
        let path = bin.join("docker");
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        bin.to_str().unwrap().into()
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
fn success(output: &Output) {
    assert!(output.status.success(), "{}", text(output));
}
fn failure(output: &Output, message: &str) {
    assert_eq!(output.status.code(), Some(1), "{}", text(output));
    assert!(text(output).contains(message), "{}", text(output));
}

#[test]
fn saved_configuration_precedence_and_secret_redaction() {
    let sandbox = Sandbox::new();
    sandbox.save(&json!({"version":1,"redis_url":"redis://127.0.0.1:1/0","namespace":"saved-invalid namespace","owned":null}));
    failure(
        &sandbox.run(&["status", "1"], None, &[]),
        "invalid job namespace",
    );
    failure(
        &sandbox.run(
            &["status", "1"],
            None,
            &[("SWARMCRAWL_JOB_NAMESPACE", "env-valid")],
        ),
        "Redis connect failed",
    );
    failure(
        &sandbox.run(
            &["status", "1", "--namespace", "flag-valid"],
            None,
            &[("SWARMCRAWL_JOB_NAMESPACE", "invalid environment")],
        ),
        "Redis connect failed",
    );
    let output = sandbox.run(
        &["check"],
        None,
        &[("SWARMCRAWL_REDIS_URL", "redis://:fixture-secret@host/bad")],
    );
    failure(&output, "invalid Redis URL");
    assert!(!text(&output).contains("fixture-secret"));
    failure(
        &sandbox.run(
            &["check", "--redis-url", "redis://127.0.0.1:1/0"],
            None,
            &[("SWARMCRAWL_REDIS_URL", "invalid")],
        ),
        "Redis connect failed",
    );
    sandbox.save(&json!({"version":1,"redis_url":"fixture-sensitive-invalid-url","namespace":"valid","owned":null}));
    failure(
        &sandbox.run(
            &["check"],
            None,
            &[("SWARMCRAWL_REDIS_URL", "redis://127.0.0.1:1/0")],
        ),
        "Redis connect failed",
    );
    let output = sandbox.run(&["check"], None, &[]);
    failure(&output, "invalid Redis URL");
    assert!(!text(&output).contains("fixture-sensitive"));
    fs::write(
        sandbox.config().join("config.json"),
        b"fixture-secret malformed JSON",
    )
    .unwrap();
    let output = sandbox.run(&["check"], None, &[]);
    failure(&output, "invalid saved configuration JSON");
    assert!(!text(&output).contains("fixture-secret"));
}

#[test]
fn failed_join_never_saves_or_needs_docker_and_existing_state_is_preserved() {
    let sandbox = Sandbox::new();
    let path = sandbox.fake_docker("echo Docker-must-not-run >&2; exit 99");
    let output = sandbox.run(
        &[
            "cluster",
            "join",
            "127.0.0.1",
            "--port",
            "1",
            "--password-stdin",
        ],
        Some(b"fixture-secret\n"),
        &[("PATH", &path)],
    );
    failure(&output, "Redis connect failed");
    assert!(!text(&output).contains("fixture-secret"));
    assert!(!text(&output).contains("Docker-must-not-run"));
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = silent.local_addr().unwrap().port().to_string();
    failure(
        &sandbox.run(
            &[
                "cluster",
                "join",
                "127.0.0.1",
                "--port",
                &port,
                "--password-stdin",
                "--redis-timeout-secs",
                "1",
            ],
            Some(b"fixture-secret\n"),
            &[("PATH", &path)],
        ),
        "Redis check timed out",
    );
    assert!(!sandbox.config().join("config.json").exists());
    failure(
        &sandbox.run(&["cluster", "join", "127.0.0.1"], None, &[]),
        "--password-stdin",
    );
    sandbox.save(&json!({"version":1,"redis_url":"redis://127.0.0.1:1/0","namespace":"retained","owned":null}));
    let before = sandbox.saved();
    for args in [
        vec!["cluster", "init", "127.0.0.1"],
        vec!["cluster", "join", "127.0.0.1"],
    ] {
        failure(
            &sandbox.run(&args, None, &[("PATH", &path)]),
            "already exists",
        );
        assert!(sandbox.saved() == before, "saved configuration changed");
    }
    for action in ["start", "stop"] {
        failure(
            &sandbox.run(&["cluster", action], None, &[("PATH", &path)]),
            "only its owner",
        );
    }
}

#[test]
fn init_requires_an_explicit_ip_and_user_password_before_creating_a_deployment() {
    let sandbox = Sandbox::new();
    for args in [
        vec!["cluster", "init"],
        vec!["cluster", "init", "not-an-ip"],
        vec!["cluster", "credential"],
    ] {
        assert_eq!(sandbox.run(&args, None, &[]).status.code(), Some(2));
    }
    let path = sandbox.fake_docker("echo linux");
    failure(
        &sandbox.run(&["cluster", "init", "0.0.0.0"], None, &[("PATH", &path)]),
        "never all interfaces",
    );
    failure(
        &sandbox.run(&["cluster", "init", "127.0.0.1"], None, &[("PATH", &path)]),
        "--password-stdin",
    );
    for password in [b"\n".as_slice(), b"bad\0fixture\n", &vec![b'x'; 4097]] {
        failure(
            &sandbox.init("6379", password, &[("PATH", &path)]),
            "password must contain",
        );
    }
    assert!(!sandbox.config().join("config.json").exists());
}

#[test]
fn docker_failures_are_actionable_private_and_recoverable_without_reinitialization() {
    let sandbox = Sandbox::new();
    failure(
        &sandbox.init("6379", b"fixture-secret\n", &[("PATH", "/nonexistent")]),
        "cannot execute Docker",
    );
    let path = sandbox.fake_docker("echo fixture-sensitive-daemon-error >&2; exit 1");
    let output = sandbox.init("6379", b"fixture-secret\n", &[("PATH", &path)]);
    failure(
        &output,
        "Docker command failed during daemon check (docker info) (exit 1)",
    );
    assert!(text(&output).contains("[unclassified]"));
    assert!(!text(&output).contains("fixture-sensitive"));
    assert!(!sandbox.config().join("config.json").exists());
    let path = sandbox.fake_docker("if [ \"$1\" = info ]; then echo linux; exit 0; fi\nif [ \"$2\" = ls ]; then exit 0; fi\nprintf '%s\\n' \"$@\" > \"$SWARMCRAWL_CONFIG_DIR/args\"\necho fixture-sensitive-error >&2\nexit 1");
    let password = b"fixture :\"\\#%@ secret\n";
    let output = sandbox.init("6379", password, &[("PATH", &path)]);
    failure(&output, "Setup incomplete");
    assert!(text(&output).contains("container creation (docker create)"));
    let saved = sandbox.saved();
    assert_eq!(saved["owned"]["bind"], "127.0.0.1");
    let info =
        redis::IntoConnectionInfo::into_connection_info(saved["redis_url"].as_str().unwrap())
            .unwrap();
    assert!(
        info.redis_settings().password().unwrap().as_bytes() == &password[..password.len() - 1]
    );
    assert!(!text(&output).contains("fixture"));
    let args = fs::read_to_string(sandbox.config().join("args")).unwrap();
    assert!(!args.contains("fixture"));
    assert!(args.contains("127.0.0.1:6379:6379"));
    let config = fs::read_to_string(sandbox.config().join("redis.conf")).unwrap();
    assert!(config.contains("requirepass \"\\x66"));
    assert!(!config.contains("fixture"));
    for name in ["config.json", "redis.conf"] {
        assert_eq!(
            fs::metadata(sandbox.config().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    failure(
        &sandbox.init("6379", password, &[("PATH", &path)]),
        "already exists",
    );
    failure(
        &sandbox.run(&["cluster", "start"], None, &[("PATH", &path)]),
        "Docker command failed",
    );
    assert!(sandbox.saved() == saved, "saved configuration changed");
    let path = sandbox.fake_docker(&format!("if [ \"$1\" = info ]; then echo linux; elif [ \"$2\" = ls ]; then echo {}; else echo '{{}}'; fi", "a".repeat(64)));
    for args in [
        &["cluster", "start"][..],
        &["cluster", "stop", "--yes"],
        &["cluster", "remove", "--yes"],
    ] {
        failure(
            &sandbox.run(args, None, &[("PATH", &path)]),
            "ownership labels do not match",
        );
        assert!(
            sandbox.saved() == saved,
            "ownership failure changed saved configuration"
        );
    }
    // A Docker removal failure must retain local recovery metadata and credentials.
    let token = saved["owned"]["token"].as_str().unwrap();
    let path = sandbox.fake_docker(&format!("if [ \"$1\" = info ]; then echo linux; elif [ \"$2\" = ls ]; then echo {}; elif [ \"$1\" = container ]; then case \"$4\" in *Labels*) echo '{{\"org.swarmcrawl.owner\":\"{token}\",\"org.swarmcrawl.managed\":\"v1\"}}';; *) echo false;; esac; else exit 1; fi", "a".repeat(64)));
    failure(
        &sandbox.run(&["cluster", "remove", "--yes"], None, &[("PATH", &path)]),
        "Docker command failed",
    );
    assert!(
        sandbox.saved() == saved,
        "failed Docker removal lost metadata"
    );
    assert!(sandbox.config().join("redis.conf").exists());
}

#[test]
fn known_docker_failures_show_the_operation_and_safe_guidance_not_raw_output() {
    let sandbox = Sandbox::new();
    for (diagnostic, category) in [
        (
            "Ports are not available: bind: cannot assign requested address",
            "[bind-address]",
        ),
        ("port is already allocated", "[host-port]"),
        (
            "invalid mount config for type bind: bind source path does not exist",
            "[config-mount]",
        ),
        (
            "error getting credentials - err: exit status 1",
            "[image-access]",
        ),
    ] {
        let path = sandbox.fake_docker(&format!(
            "if [ \"$1\" = info ]; then echo linux; exit 0; fi\nif [ \"$2\" = ls ]; then exit 0; fi\nprintf '%s\\n' '{diagnostic}' 'redis://user:fixture-secret@host/0' '/fixture-private-path' >&2\necho fixture-sensitive-stdout\nexit 125"
        ));
        let output = if sandbox.config().join("config.json").exists() {
            sandbox.run(&["cluster", "start"], None, &[("PATH", &path)])
        } else {
            sandbox.init("6379", b"fixture-secret\n", &[("PATH", &path)])
        };
        failure(&output, "container creation (docker create) (exit 125)");
        assert!(text(&output).contains(category));
        assert!(!text(&output).contains("fixture-"));
        assert!(output.stdout.is_empty());
        assert!(sandbox.config().join("config.json").exists());
        assert!(sandbox.config().join("redis.conf").exists());
    }
}

#[test]
fn info_is_offline_secret_free_and_marks_defaults_by_value() {
    let sandbox = Sandbox::new();
    failure(
        &sandbox.run(&["cluster", "info"], None, &[]),
        "no saved connection",
    );
    sandbox.save(&json!({"version":1,"redis_url":"redis://:fixture-secret@127.0.0.1:6379/0","namespace":"swarmcrawl:v1","owned":null}));
    let output = sandbox.run(
        &["cluster", "info", "--namespace", "override"],
        None,
        &[("PATH", "/nonexistent")],
    );
    success(&output);
    let output = text(&output);
    for line in [
        "IP: 127.0.0.1 (default)",
        "Port: 6379 (default)",
        "Database: 0 (default)",
        "Namespace: swarmcrawl:v1 (default)",
    ] {
        assert!(output.contains(line));
    }
    assert!(!output.contains("fixture-secret"));
    sandbox.save(&json!({"version":1,"redis_url":"redis://user:fixture-secret@[::1]:6380/3","namespace":"other","owned":null}));
    let output = sandbox.run(&["cluster", "info"], None, &[]);
    success(&output);
    let output = text(&output);
    for line in ["IP: ::1", "Port: 6380", "Database: 3", "Namespace: other"] {
        assert!(output.contains(line));
    }
    assert!(!output.contains("(default)"));
    assert!(!output.contains("fixture-secret"));
    sandbox.save(&json!({"version":1,"redis_url":"redis://fixture-user:fixture-secret@redis.example.invalid/0","namespace":"other","owned":null}));
    let output = sandbox.run(&["cluster", "info"], None, &[]);
    success(&output);
    assert!(text(&output).contains("Host: redis.example.invalid"));
    assert!(!text(&output).contains("fixture-"));
}

#[test]
fn remove_joined_connection_is_local_confirmed_and_preserves_unrelated_files() {
    let sandbox = Sandbox::new();
    sandbox.save(&json!({"version":1,"redis_url":"redis://:fixture-secret@127.0.0.1:1/0","namespace":"valid","owned":null}));
    fs::write(sandbox.config().join("user-notes"), b"must stay").unwrap();
    failure(
        &sandbox.run(&["cluster", "remove"], None, &[("PATH", "/nonexistent")]),
        "remove canceled",
    );
    assert!(sandbox.config().join("config.json").exists());
    // Lifecycle lock is stable across removal/re-init, not deleted under waiters.
    let lock_path = sandbox.config().join("deployment.lock");
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    lock.try_lock().unwrap();
    failure(
        &sandbox.run(&["cluster", "remove", "--yes"], None, &[]),
        "another cluster command",
    );
    drop(lock);
    let output = sandbox.run(
        &["cluster", "remove", "--yes"],
        None,
        &[("PATH", "/nonexistent")],
    );
    success(&output);
    assert!(text(&output).contains("Remote Redis and its jobs are NOT changed"));
    assert!(!sandbox.config().join("config.json").exists());
    assert!(!sandbox.config().join("redis.conf").exists());
    assert_eq!(
        fs::read(sandbox.config().join("user-notes")).unwrap(),
        b"must stay"
    );
    assert!(lock_path.exists());
    assert_eq!(fs::read_dir(sandbox.config()).unwrap().count(), 2); // No backup.
    success(&sandbox.run(&["cluster", "remove", "--yes"], None, &[]));
    let path = sandbox.fake_docker("echo linux");
    failure(
        &sandbox.init("6379", b"\n", &[("PATH", &path)]),
        "password must contain",
    ); // Init is no longer blocked by old settings.
}

#[test]
fn private_storage_rejects_symlinks_and_shared_permissions() {
    let sandbox = Sandbox::new();
    sandbox.save(
        &json!({"version":1,"redis_url":"redis://localhost/0","namespace":"valid","owned":null}),
    );
    let path = sandbox.config().join("config.json");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    failure(&sandbox.run(&["check"], None, &[]), "mode 600");
    fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(Path::new("/dev/null"), &path).unwrap();
    failure(&sandbox.run(&["check"], None, &[]), "not symlinked");
    fs::remove_file(&path).unwrap();
    let outside = sandbox.root.path().join("must-not-create");
    std::os::unix::fs::symlink(&outside, sandbox.config().join("deployment.lock")).unwrap();
    failure(
        &sandbox.run(&["cluster", "init", "127.0.0.1"], None, &[]),
        "not symlinked",
    );
    assert!(!outside.exists());
    fs::remove_file(sandbox.config().join("deployment.lock")).unwrap();
    sandbox.save(
        &json!({"version":1,"redis_url":"redis://localhost/0","namespace":"valid","owned":null}),
    );
    std::os::unix::fs::symlink(&outside, sandbox.config().join("redis.conf")).unwrap();
    failure(
        &sandbox.run(&["cluster", "remove", "--yes"], None, &[]),
        "not symlinked",
    );
    assert!(path.exists());
    fs::remove_dir_all(sandbox.config()).unwrap();
    std::os::unix::fs::symlink(&outside, sandbox.config()).unwrap();
    failure(&sandbox.run(&["check"], None, &[]), "not symlinked");
}

fn timeout_command() -> Command {
    let path = std::env::var_os("PATH").expect("PATH for GNU timeout");
    let executable = std::env::split_paths(&path)
        .map(|directory| directory.join("timeout"))
        .find(|path| path.is_file())
        .expect("setup CLI tests require GNU timeout on PATH");
    Command::new(executable)
}
fn docker(args: &[&str]) -> Output {
    timeout_command()
        .args(["130s", "docker"])
        .args(args)
        .output()
        .unwrap()
}

// Cleanup after assertions as well as success. Saved ownership is retained until
// the product has removed Docker resources; test-created unrelated containers use IDs.
struct DockerCleanup {
    configurations: Vec<PathBuf>,
    ids: Vec<String>,
}
impl Drop for DockerCleanup {
    fn drop(&mut self) {
        let mut failed = false;
        for directory in &self.configurations {
            let Ok(bytes) = fs::read(directory.join("config.json")) else {
                continue;
            };
            let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            let Some(token) = value["owned"]["token"].as_str() else {
                continue;
            };
            let name = format!("swarmcrawl-{token}");
            let label = docker(&[
                "inspect",
                "--format",
                "{{index .Config.Labels \"org.swarmcrawl.owner\"}}",
                &name,
            ]);
            if label.status.success()
                && String::from_utf8_lossy(&label.stdout).trim() == token
                && !docker(&["rm", "--force", "--volumes", &name])
                    .status
                    .success()
            {
                eprintln!("integration cleanup failed for owned container {name}");
                failed = true;
            }
        }
        for id in &self.ids {
            if !docker(&["rm", "--force", "--volumes", id]).status.success() {
                eprintln!("integration cleanup failed for test-created container {id}");
                failed = true;
            }
        }
        if failed && !std::thread::panicking() {
            panic!("integration container cleanup incomplete; see diagnostics");
        }
    }
}
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
#[ignore = "requires local Linux Docker daemon, redis:7.4-alpine access and GNU timeout"]
fn real_user_password_join_remove_reinit_and_owned_disposable_lifecycle() {
    success(&docker(&["info"]));
    let owner = Sandbox::new();
    let joined = Sandbox::new();
    let wrong = Sandbox::new();
    let conflict = Sandbox::new();
    let mut cleanup = DockerCleanup {
        configurations: vec![owner.config(), conflict.config()],
        ids: vec![],
    };
    let port = free_port().to_string();
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).unwrap();
    // Supplied via stdin, never Docker/CLI arguments. Exercise exact Redis config
    // quoting and URL encoding, including whitespace, Unicode and metacharacters.
    let password = format!(
        "{} :\"\\#%@/é\t \n",
        random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let password_value = password.strip_suffix('\n').unwrap();
    let output = owner.run(
        &[
            "cluster",
            "init",
            "127.0.0.1",
            "--port",
            &port,
            "--database",
            "3",
            "--namespace",
            "integration:shared",
            "--password-stdin",
        ],
        Some(password.as_bytes()),
        &[],
    );
    success(&output);
    assert!(text(&output).contains("This PC only"));
    let saved = owner.saved();
    let name = format!("swarmcrawl-{}", saved["owned"]["token"].as_str().unwrap());
    assert!(!text(&output).contains(password_value));
    let inspect = docker(&["inspect", &name]);
    success(&inspect);
    assert!(
        !text(&inspect).contains(password_value),
        "secret in Docker arguments/metadata"
    );
    assert!(text(&docker(&["exec", &name, "redis-cli", "ping"])).contains("NOAUTH"));
    failure(
        &owner.run(
            &[
                "check",
                "--redis-url",
                &format!("redis://127.0.0.1:{port}/3"),
            ],
            None,
            &[],
        ),
        "Redis connect failed",
    );
    success(&owner.run(&["check"], None, &[]));
    let args = [
        "cluster",
        "join",
        "127.0.0.1",
        "--port",
        &port,
        "--database",
        "3",
        "--namespace",
        "integration:shared",
        "--password-stdin",
    ];
    let output = wrong.run(&args, Some(b"deliberately-wrong-fixture\n"), &[]);
    failure(&output, "Redis connect failed");
    assert!(!text(&output).contains("deliberately-wrong-fixture"));
    assert!(!wrong.config().join("config.json").exists());
    let output = joined.run(
        &args,
        Some(password.as_bytes()),
        &[("PATH", "/nonexistent")],
    );
    success(&output);
    assert!(!text(&output).contains(password_value));
    assert!(text(&output).contains("does not verify namespace"));
    let info = owner.run(&["cluster", "info"], None, &[("PATH", "/nonexistent")]);
    success(&info);
    assert!(text(&info).contains("Namespace: integration:shared"));
    assert!(!text(&info).contains(password_value));
    success(&owner.run(&["submit", "https://example.invalid/docs/"], None, &[]));
    assert!(text(&joined.run(&["status", "1"], None, &[])).contains("frontier 1"));
    failure(
        &owner.init(&port, password.as_bytes(), &[]),
        "already exists",
    );
    success(&owner.run(&["cluster", "start"], None, &[]));
    success(&joined.run(&["status", "1"], None, &[]));
    for action in ["stop", "remove"] {
        failure(
            &owner.run(&["cluster", action], None, &[]),
            &format!("{action} canceled"),
        );
        success(&joined.run(&["status", "1"], None, &[]));
    }
    failure(
        &joined.run(&["cluster", "stop", "--yes"], None, &[]),
        "only its owner",
    );
    let conflict_output = conflict.init(&port, password.as_bytes(), &[]);
    failure(&conflict_output, "Setup incomplete");
    assert!(text(&conflict_output).contains("container start (docker start)"));
    assert!(text(&conflict_output).contains("[host-port]"));
    assert!(!text(&conflict_output).contains(password_value));
    success(&owner.run(&["cluster", "stop", "--yes"], None, &[]));
    success(&owner.run(&["cluster", "stop", "--yes"], None, &[]));
    success(&conflict.run(&["cluster", "stop", "--yes"], None, &[]));
    success(&conflict.run(&["cluster", "start"], None, &[]));
    success(&conflict.run(&["cluster", "remove", "--yes"], None, &[]));
    assert!(!conflict.config().join("config.json").exists());
    success(&owner.run(&["cluster", "start"], None, &[]));
    failure(&joined.run(&["status", "1"], None, &[]), "unknown job");
    assert!(owner.saved() == saved, "saved configuration changed");

    let unrelated = Sandbox::new();
    let other_token = format!(
        "{:032x}",
        u128::from(free_port()) + (u128::from(std::process::id()) << 32)
    );
    let mut other_saved = saved;
    other_saved["owned"]["token"] = json!(other_token);
    unrelated.save(&other_saved);
    let other_name = format!("swarmcrawl-{other_token}");
    let created = docker(&["create", "--name", &other_name, "redis:7.4-alpine"]);
    success(&created);
    let id = String::from_utf8(created.stdout)
        .unwrap()
        .trim()
        .to_string();
    cleanup.ids.push(id.clone());
    for action in ["start", "stop", "remove"] {
        failure(
            &unrelated.run(&["cluster", action], None, &[]),
            "ownership labels",
        );
        assert!(unrelated.config().join("config.json").exists());
    }
    assert_eq!(
        String::from_utf8_lossy(&docker(&["inspect", "--format", "{{.State.Status}}", &id]).stdout)
            .trim(),
        "created"
    );
    success(&joined.run(
        &["cluster", "remove", "--yes"],
        None,
        &[("PATH", "/nonexistent")],
    ));
    success(&owner.run(&["check"], None, &[])); // Joining PC removal did not stop Redis.
    success(&joined.run(&args, Some(password.as_bytes()), &[])); // Can join again.
    success(&owner.run(&["cluster", "remove", "--yes"], None, &[]));
    assert!(!docker(&["inspect", &name]).status.success());
    assert_eq!(fs::read_dir(owner.config()).unwrap().count(), 1); // Only stable lock; no backups/secrets.
    failure(
        &owner.run(&["cluster", "info"], None, &[]),
        "no saved connection",
    );
    let hex_password = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        .repeat(2);
    success(&owner.init(&port, hex_password.as_bytes(), &[]));
    assert_eq!(owner.saved()["namespace"], "swarmcrawl:v1");
    success(&owner.run(&["cluster", "stop", "--yes"], None, &[]));
    // Older generated-password deployments used a bare 64-hex requirepass value.
    // They must still start without rewriting their existing private config file.
    let legacy = format!(
        "bind 0.0.0.0\nprotected-mode yes\nport 6379\nsave \"\"\nappendonly no\nrequirepass {hex_password}\nlogfile \"\"\n"
    );
    fs::write(owner.config().join("redis.conf"), &legacy).unwrap();
    success(&owner.run(&["cluster", "start"], None, &[]));
    assert!(fs::read(owner.config().join("redis.conf")).unwrap() == legacy.as_bytes());
    success(&owner.run(&["check"], None, &[]));
    success(&owner.run(&["cluster", "remove", "--yes"], None, &[]));
    // Verify the advertised UTF-8 byte limit against Redis, not just validation.
    let maximum_password = "🚀".repeat(1024);
    success(&owner.init(&port, maximum_password.as_bytes(), &[]));
    success(&owner.run(&["check"], None, &[]));
    success(&owner.run(&["cluster", "remove", "--yes"], None, &[]));
}

#[test]
#[ignore = "requires Python 3 on Unix for bounded pseudo-terminal interaction (no Docker daemon needed)"]
fn interactive_passwords_and_destructive_confirmations() {
    let output = timeout_command()
        .args(["60s", "python3", "-B"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/test-cluster-terminal.py"))
        .arg(env!("CARGO_BIN_EXE_swarmcrawl"))
        .output()
        .unwrap();
    success(&output);
}
