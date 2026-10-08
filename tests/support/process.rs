//! Bounded actual CLI processes, incremental pipe capture and owned-child cleanup.

use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use super::support::Context;

pub struct Process {
    _config_dir: tempfile::TempDir,
    child: Child,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    readers: Vec<thread::JoinHandle<()>>,
}

pub struct Output {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

fn capture(pipe: impl Read + Send + 'static) -> (Arc<Mutex<String>>, thread::JoinHandle<()>) {
    let log = Arc::new(Mutex::new(String::new()));
    let owned = log.clone();
    let reader = thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            let line = line.expect("read CLI pipe");
            let mut log = owned.lock().unwrap();
            assert!(log.len() + line.len() < 64 * 1024, "bounded CLI pipe");
            log.push_str(&line);
            log.push('\n');
        }
    });
    (log, reader)
}

impl Process {
    pub fn start(context: &Context, args: &[&str]) -> Self {
        Self::with_env(context, args, &[])
    }

    pub fn with_env(context: &Context, args: &[&str], env: &[(&str, &str)]) -> Self {
        let config_dir = tempfile::tempdir().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmcrawl"));
        command
            .args(args)
            .env("SWARMCRAWL_CONFIG_DIR", config_dir.path().join("absent"))
            // Retain only this suite's explicitly supplied Redis endpoint.
            .env("SWARMCRAWL_REDIS_TIMEOUT_SECS", "5")
            .env("SWARMCRAWL_JOB_NAMESPACE", &context.namespace)
            .env_remove("SWARMCRAWL_FETCH_TIMEOUT_SECS")
            .env_remove("SWARMCRAWL_DIAGNOSTICS")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command.spawn().expect("start actual swarmcrawl binary");
        let (stdout, out_reader) = capture(child.stdout.take().unwrap());
        let (stderr, err_reader) = capture(child.stderr.take().unwrap());
        Self {
            _config_dir: config_dir,
            child,
            stdout,
            stderr,
            readers: vec![out_reader, err_reader],
        }
    }

    pub fn stdout(&self) -> String {
        self.stdout.lock().unwrap().clone()
    }

    pub fn stderr(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }

    pub fn running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    pub async fn wait_stdout(&mut self, message: &str) {
        self.wait_log(message, true).await;
    }

    pub async fn wait_stderr(&mut self, message: &str) {
        self.wait_log(message, false).await;
    }

    async fn wait_log(&mut self, message: &str, stdout: bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let log = if stdout { self.stdout() } else { self.stderr() };
                if log.contains(message) {
                    break;
                }
                assert!(
                    self.running(),
                    "CLI exited early; stdout: {}; stderr: {}",
                    self.stdout(),
                    self.stderr()
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bounded CLI update wait");
    }

    #[cfg(unix)]
    pub fn interrupt(&self) {
        assert!(
            Command::new("timeout")
                .args(["5s", "kill", "-INT", &self.child.id().to_string()])
                .status()
                .expect("send owned child SIGINT")
                .success()
        );
    }

    pub async fn exit(&mut self) -> Output {
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bounded CLI exit");
        for reader in self.readers.drain(..) {
            reader.join().unwrap();
        }
        Output {
            code: status.code(),
            stdout: self.stdout(),
            stderr: self.stderr(),
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // On assertion/deadline, kill/reap only this test's child before scoped
        // Redis cleanup; no subprocess or job state is reused/reset.
        let _ = self.child.kill();
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

pub async fn run(context: &Context, args: &[&str]) -> Output {
    Process::start(context, args).exit().await
}
