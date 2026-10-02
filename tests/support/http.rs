//! Scripted loopback HTTP/1 fixture: real request logs, response gates and RAII cleanup.

use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use swarmcrawl::urls::{CrawlScope, CrawlUrl};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub prefix: Vec<u8>,
    pub tail: Vec<u8>,
    pub body_gate: Option<Arc<Semaphore>>,
    pub header_gate: Option<Arc<Semaphore>>,
}

impl Reply {
    pub fn new(status: u16, mime: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".into(), mime.into())],
            prefix: body.into(),
            tail: Vec::new(),
            body_gate: None,
            header_gate: None,
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn gated_body(mut self, tail: impl Into<Vec<u8>>, gate: &Arc<Semaphore>) -> Self {
        self.tail = tail.into();
        self.body_gate = Some(gate.clone());
        self
    }

    pub fn gated_headers(mut self, gate: &Arc<Semaphore>) -> Self {
        self.header_gate = Some(gate.clone());
        self
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub target: String,
    pub head: String,
}

#[derive(Default)]
struct State {
    requests: Mutex<Vec<Request>>,
    errors: Mutex<Vec<String>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    headers_sent: AtomicUsize,
    completed: AtomicUsize,
}

pub struct HttpFixture {
    base: CrawlUrl,
    state: Arc<State>,
    server: JoinHandle<()>,
}

impl HttpFixture {
    pub async fn start(routes: impl IntoIterator<Item = (String, Reply)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = CrawlUrl::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let routes = Arc::new(routes.into_iter().collect::<HashMap<_, _>>());
        let state = Arc::new(State::default());
        let owned = state.clone();
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let routes = routes.clone();
                        let state = owned.clone();
                        connections.spawn(async move {
                            if let Err(error) = serve(stream, &routes, &state).await
                                && !matches!(error.kind(), io::ErrorKind::BrokenPipe
                                    | io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted)
                            {
                                state.errors.lock().unwrap().push(error.to_string());
                            }
                        });
                    }
                    joined = connections.join_next(), if !connections.is_empty() => {
                        joined.unwrap().unwrap();
                    }
                }
            }
            // Aborting this task drops JoinSet and aborts all owned connections.
        });
        Self {
            base,
            state,
            server,
        }
    }

    pub fn url(&self, path: &str) -> CrawlUrl {
        self.base.resolve(path).unwrap()
    }

    pub fn scope(&self, path: &str) -> CrawlScope {
        CrawlScope::new(self.url(path))
    }

    pub fn requests(&self) -> Vec<Request> {
        self.state.requests.lock().unwrap().clone()
    }

    pub fn peak(&self) -> usize {
        self.state.peak.load(Ordering::SeqCst)
    }

    pub async fn wait_for_requests(&self, count: usize) {
        wait_until(|| self.requests().len() >= count).await;
    }

    pub async fn wait_for_headers(&self, count: usize) {
        wait_until(|| self.state.headers_sent.load(Ordering::SeqCst) >= count).await;
    }

    pub async fn wait_for_completed(&self, count: usize) {
        wait_until(|| self.state.completed.load(Ordering::SeqCst) >= count).await;
    }

    pub fn assert_healthy(&self) {
        assert!(
            !self.server.is_finished(),
            "HTTP fixture server stopped unexpectedly"
        );
        assert!(
            self.state.errors.lock().unwrap().is_empty(),
            "HTTP fixture errors"
        );
        assert!(
            self.requests()
                .iter()
                .all(|request| request.method == "GET")
        );
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Active<'a>(&'a State);

impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.completed.fetch_add(1, Ordering::SeqCst);
    }
}

async fn serve(
    mut stream: TcpStream,
    routes: &HashMap<String, Reply>,
    state: &State,
) -> io::Result<()> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    while !bytes.windows(4).any(|chunk| chunk == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(()); // Aborted caller before sending a request.
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized fixture request",
            ));
        }
    }
    let head = String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 fixture request"))?;
    let mut request_line = head.lines().next().unwrap().split_whitespace();
    let method = request_line.next().unwrap().to_owned();
    let target = request_line.next().unwrap().to_owned();
    state.requests.lock().unwrap().push(Request {
        method,
        target: target.clone(),
        head,
    });
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.peak.fetch_max(active, Ordering::SeqCst);
    let _active = Active(state);
    let default = Reply::new(404, "text/plain", "Not found");
    let reply = routes.get(&target).unwrap_or(&default);
    if reply.status == 0 {
        return Ok(()); // Deliberately disconnect without an HTTP response.
    }
    if let Some(gate) = &reply.header_gate
        && !wait_for_gate_or_disconnect(&mut stream, gate).await?
    {
        return Ok(());
    }
    let mut headers = format!("HTTP/1.1 {} Fixture\r\nConnection: close\r\n", reply.status);
    for (name, value) in &reply.headers {
        headers.push_str(&format!("{name}: {value}\r\n"));
    }
    if !reply
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
    {
        headers.push_str(&format!(
            "Content-Length: {}\r\n",
            reply.prefix.len() + reply.tail.len()
        ));
    }
    headers.push_str("\r\n");
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&reply.prefix).await?;
    state.headers_sent.fetch_add(1, Ordering::SeqCst);
    if let Some(gate) = &reply.body_gate
        && !wait_for_gate_or_disconnect(&mut stream, gate).await?
    {
        return Ok(());
    }
    stream.write_all(&reply.tail).await?;
    stream.shutdown().await
}

async fn wait_for_gate_or_disconnect(stream: &mut TcpStream, gate: &Semaphore) -> io::Result<bool> {
    let mut byte = [0];
    tokio::select! {
        permit = gate.acquire() => {
            permit.unwrap().forget();
            Ok(true)
        }
        read = stream.read(&mut byte) => {
            assert_eq!(read?, 0, "unexpected request body in fixture");
            Ok(false)
        }
    }
}

pub async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded HTTP fixture polling");
}
