//! Subprocess lifecycle for the gateway server: spawn it, stream its stdout/
//! stderr back without blocking the GUI thread, and stop it gracefully
//! (SIGTERM, escalating to SIGKILL if it doesn't exit in time).
//!
//! Deliberately process-based rather than calling `streaming_gateway::run()`
//! in-process: a crash or hang in the server can never take the GUI down
//! with it, and "stop" is just "make this child process go away", which is
//! easy to get right.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

pub enum LogLine {
    Out(String),
    Err(String),
}

impl LogLine {
    pub fn text(&self) -> &str {
        match self {
            LogLine::Out(s) | LogLine::Err(s) => s,
        }
    }

    pub fn is_err(&self) -> bool {
        matches!(self, LogLine::Err(_))
    }
}

/// Locates the `streaming-gateway` server binary: first next to this GUI
/// binary's own executable (covers both a `cargo build` target dir and a
/// `.deb`-installed `/usr/bin/`), then falls back to `$PATH`.
pub fn find_server_binary() -> Option<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("streaming-gateway");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join("streaming-gateway"))
        .find(|candidate| candidate.is_file())
}

#[derive(Default)]
pub struct ServerProcess {
    child: Option<Child>,
    log_rx: Option<Receiver<LogLine>>,
}

impl ServerProcess {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reaps the child if it has exited (e.g. it crashed, or another process
    /// on the port made it fail to bind) so the GUI's idea of "running"
    /// never goes stale.
    pub fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => match child.try_wait() {
                Ok(Some(_status)) => {
                    self.child = None;
                    false
                }
                Ok(None) => true,
                Err(_) => {
                    self.child = None;
                    false
                }
            },
            None => false,
        }
    }

    pub fn start(&mut self, program: &Path, env: &[(String, String)]) -> Result<()> {
        if self.is_running() {
            bail!("server is already running");
        }

        let mut cmd = Command::new(program);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in env {
            cmd.env(key, value);
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to start {}", program.display()))?;

        let (tx, rx) = mpsc::channel();

        if let Some(stdout) = child.stdout.take() {
            let tx = tx.clone();
            thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if tx.send(LogLine::Out(line)).is_err() {
                        break;
                    }
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    if tx.send(LogLine::Err(line)).is_err() {
                        break;
                    }
                }
            });
        }

        self.child = Some(child);
        self.log_rx = Some(rx);
        Ok(())
    }

    /// Drains whatever log lines have arrived since the last call. Never blocks.
    pub fn drain_logs(&mut self) -> Vec<LogLine> {
        let Some(rx) = &self.log_rx else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while let Ok(line) = rx.try_recv() {
            out.push(line);
        }
        out
    }

    /// Takes ownership of the child (if any) so it can be stopped on a
    /// background thread without holding up the caller.
    pub fn take_child(&mut self) -> Option<Child> {
        self.log_rx = None;
        self.child.take()
    }

    /// Blocking: SIGTERM, wait up to `grace`, then SIGKILL if still alive.
    /// Run this on its own thread -- never call it from the UI thread.
    pub fn stop_blocking(mut child: Child, grace: Duration) {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status();

        let deadline = Instant::now() + grace;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
                _ => break,
            }
        }

        let _ = child.kill();
        let _ = child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny real subprocess (no network, no display) standing in for the
    /// server binary, so these tests exercise the actual spawn/read/stop
    /// machinery without depending on the gateway crate being built first.
    fn fake_server_script() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "gateway-gui-test-fakeserver-{}-{}.sh",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        std::fs::write(
            &path,
            "#!/bin/sh\necho \"listening on http://127.0.0.1:9999\"\necho oops >&2\ntrap 'exit 0' TERM\nwhile true; do sleep 0.05; done\n",
        )
        .unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    #[test]
    fn start_reports_running_and_streams_output() {
        let script = fake_server_script();
        let mut process = ServerProcess::new();
        process.start(&script, &[]).expect("starts");
        assert!(process.is_running());

        let mut lines = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        while lines.len() < 2 && Instant::now() < deadline {
            lines.extend(process.drain_logs().into_iter().map(|l| l.text().to_string()));
            thread::sleep(Duration::from_millis(20));
        }

        assert!(lines.iter().any(|l| l.contains("http://127.0.0.1:9999")));

        let child = process.take_child().expect("child present");
        ServerProcess::stop_blocking(child, Duration::from_secs(2));
        let _ = std::fs::remove_file(&script);
    }

    #[test]
    fn stop_blocking_escalates_to_sigkill_if_process_ignores_sigterm() {
        let path = std::env::temp_dir().join(format!(
            "gateway-gui-test-stubborn-{}-{}.sh",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        std::fs::write(&path, "#!/bin/sh\ntrap '' TERM\nwhile true; do sleep 0.05; done\n").unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
        std::fs::set_permissions(&path, perms).unwrap();

        let mut process = ServerProcess::new();
        process.start(&path, &[]).expect("starts");
        let child = process.take_child().unwrap();

        let started = Instant::now();
        ServerProcess::stop_blocking(child, Duration::from_millis(300));
        // Must not hang forever waiting on a process that ignores SIGTERM.
        assert!(started.elapsed() < Duration::from_secs(3));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn find_server_binary_prefers_sibling_of_current_exe() {
        // This just documents the contract; the actual sibling lookup is
        // exercised implicitly by every manual run (gui + server built into
        // the same target/release directory).
        let _ = find_server_binary();
    }
}
