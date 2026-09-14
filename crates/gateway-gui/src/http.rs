//! Every HTTP call this window makes to its own gateway, and the long-lived
//! worker that makes them.
//!
//! All of it is loopback plain HTTP. The worker exists because the obvious
//! version -- a fresh thread and a fresh `ureq` agent per request -- ran once
//! a second for the health poll alone, and a fresh agent has no connection
//! pool, so every sample paid for a TCP handshake and a thread.
//!
//! Two lanes, each one thread with one agent that lives as long as the
//! window:
//!
//! * **poll** -- the 1 Hz `/health` read. Its agent pools the connection. A
//!   `GET` is safe to pool because ureq retries it once when a recycled
//!   connection turns out to be closed (the gateway restarted, say).
//! * **action** -- purge, export, pause/resume/delete. Its agent keeps no idle
//!   connections: ureq will *not* retry a `POST` over a stale pooled socket,
//!   so a pooled purge after a gateway restart would fail for no reason the
//!   user could see. These are rare clicks; a handshake each is nothing.
//!
//! Separate lanes so a 60-second log export never stalls the traffic graph,
//! and so `systemctl` work -- which can block for seconds -- is never queued
//! here at all (see `Job::spawn`).

use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use serde::Deserialize;

use crate::health::HealthInfo;

type Task = Box<dyn FnOnce(&ureq::Agent) + Send>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Poll,
    Action,
}

pub struct HttpWorker {
    poll: Sender<Task>,
    action: Sender<Task>,
}

impl Default for HttpWorker {
    fn default() -> Self {
        Self {
            poll: spawn_lane("http-poll", ureq::AgentBuilder::new().build()),
            action: spawn_lane(
                "http-action",
                ureq::AgentBuilder::new().max_idle_connections(0).build(),
            ),
        }
    }
}

fn spawn_lane(name: &str, agent: ureq::Agent) -> Sender<Task> {
    let (tx, rx) = mpsc::channel::<Task>();
    let spawned = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            for task in rx {
                // A panic in one request must not take the lane down with
                // it: every later request would then vanish unanswered.
                let _ = panic::catch_unwind(AssertUnwindSafe(|| task(&agent)));
            }
        });
    if let Err(e) = spawned {
        // The sender is returned anyway; with no thread behind it every
        // submission is dropped, which `Job::poll` reads as a dead worker.
        eprintln!("could not start the {name} worker: {e}");
    }
    tx
}

impl HttpWorker {
    /// Queues `work` on `lane` and hands back where its result will land.
    pub fn submit<T: Send + 'static>(
        &self,
        lane: Lane,
        work: impl FnOnce(&ureq::Agent) -> T + Send + 'static,
    ) -> Receiver<T> {
        let (tx, rx) = mpsc::channel();
        let task: Task = Box::new(move |agent| {
            let _ = tx.send(work(agent));
        });
        let sender = match lane {
            Lane::Poll => &self.poll,
            Lane::Action => &self.action,
        };
        // A failed send drops the task and with it `tx`, so the receiver
        // reports disconnected rather than waiting forever.
        let _ = sender.send(task);
        rx
    }
}

fn loopback(port: &str, path: &str) -> String {
    format!("http://127.0.0.1:{port}{path}")
}

/// `GET /health`. `None` for anything short of a parseable reply -- to this
/// window a server that answers garbage is as down as one that does not answer.
pub fn fetch_health(agent: &ureq::Agent, port: &str) -> Option<HealthInfo> {
    agent
        .get(&loopback(port, "/health"))
        .timeout(Duration::from_millis(1500))
        .call()
        .ok()
        .and_then(|resp| resp.into_json::<HealthInfo>().ok())
}

/// `POST /cache/clear`, returning how many downloads were deleted.
pub fn clear_cache(agent: &ureq::Agent, port: &str) -> Result<usize, String> {
    let resp = agent
        .post(&loopback(port, "/cache/clear"))
        .timeout(Duration::from_secs(30))
        .call()
        .map_err(describe_error)?;
    resp.into_json::<serde_json::Value>()
        .ok()
        .and_then(|v| v.get("deleted").and_then(|d| d.as_u64()))
        .map(|n| n as usize)
        .ok_or_else(|| "server gave an unreadable reply".to_string())
}

/// `GET /audit/export`, as the text that heads the saved report. Never an
/// error: a report that says why half of it is missing beats no report.
pub fn fetch_audit_export(agent: &ureq::Agent, port: &str) -> String {
    match agent
        .get(&loopback(port, "/audit/export"))
        .timeout(Duration::from_secs(60))
        .call()
    {
        Ok(resp) => resp
            .into_string()
            .unwrap_or_else(|e| format!("(could not read the log body: {e})\n")),
        Err(e) => format!(
            "(could not fetch the event log from the server: {})\n",
            describe_error(e)
        ),
    }
}

/// `POST /torrents/{hash}/{verb}`.
pub fn torrent_action(
    agent: &ureq::Agent,
    port: &str,
    hash: &str,
    verb: &str,
) -> Result<(), String> {
    agent
        .post(&loopback(port, &format!("/torrents/{hash}/{verb}")))
        .timeout(Duration::from_secs(20))
        .call()
        .map(|_| ())
        .map_err(describe_error)
}

/// A failed call, in words.
///
/// The loopback-only endpoints answer a refusal or a failure with
/// `{"error": "..."}`, which says what actually went wrong ("only available
/// from this machine", "no such torrent"); ureq's own rendering of a status
/// error is just the URL and the number.
pub fn describe_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            message_from_status(code, &body)
        }
        ureq::Error::Transport(transport) => transport.to_string(),
    }
}

fn message_from_status(code: u16, body: &str) -> String {
    #[derive(Deserialize)]
    struct ErrorBody {
        error: String,
    }
    match serde_json::from_str::<ErrorBody>(body) {
        Ok(parsed) if !parsed.error.trim().is_empty() => parsed.error,
        _ => format!("server answered HTTP {code}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Instant;

    #[test]
    fn the_servers_own_error_message_is_preferred_over_the_status_code() {
        assert_eq!(
            message_from_status(403, r#"{"error":"only available from this machine"}"#),
            "only available from this machine"
        );
        // An older server sent an empty body; say what little is known.
        assert_eq!(message_from_status(500, ""), "server answered HTTP 500");
        assert_eq!(
            message_from_status(404, "<html>not json</html>"),
            "server answered HTTP 404"
        );
        assert_eq!(
            message_from_status(500, r#"{"error":"  "}"#),
            "server answered HTTP 500"
        );
    }

    /// One tiny HTTP/1.1 server that answers `count` requests with `reply`.
    fn serve(count: usize, reply: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port().to_string();
        thread::spawn(move || {
            for stream in listener.incoming().take(count) {
                let mut stream = stream.unwrap();
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(reply.as_bytes());
            }
        });
        port
    }

    fn recv<T>(rx: Receiver<T>) -> T {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("worker answered")
    }

    #[test]
    fn a_refusal_from_a_loopback_endpoint_reaches_the_caller_in_words() {
        let body = r#"{"error":"only available from this machine"}"#;
        let reply: &'static str = Box::leak(
            format!(
                "HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .into_boxed_str(),
        );
        let port = serve(1, reply);
        let worker = HttpWorker::default();
        let outcome = recv(worker.submit(Lane::Action, move |agent| clear_cache(agent, &port)));
        assert_eq!(outcome, Err("only available from this machine".to_string()));
    }

    #[test]
    fn a_slow_action_does_not_hold_up_the_poll_lane() {
        let worker = HttpWorker::default();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let _slow = worker.submit(Lane::Action, move |_| {
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });
        let started = Instant::now();
        let answer = recv(worker.submit(Lane::Poll, |_| 42));
        assert_eq!(answer, 42);
        assert!(started.elapsed() < Duration::from_secs(2));
        let _ = release_tx.send(());
    }

    #[test]
    fn a_panicking_request_does_not_kill_its_lane() {
        let worker = HttpWorker::default();
        let crashed = worker.submit(Lane::Poll, |_| -> u8 { panic!("boom") });
        assert!(crashed.recv_timeout(Duration::from_secs(5)).is_err());
        assert_eq!(recv(worker.submit(Lane::Poll, |_| 1)), 1);
    }
}
