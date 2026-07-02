use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use server::Client;

const INDEX_HTML: &str = include_str!("ui/index.html");
const APP_CSS: &str = include_str!("ui/app.css");
const APP_JS: &str = include_str!("ui/app.js");

struct AppState {
    next_client_id: AtomicUsize,
}

impl AppState {
    fn new() -> Self {
        Self {
            next_client_id: AtomicUsize::new(10_000_000),
        }
    }

    fn with_client<T>(
        &self,
        nodes: Vec<SocketAddr>,
        f: impl FnOnce(&mut Client) -> T,
    ) -> Result<T, String> {
        let id = self
            .next_client_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut client = Client::new(nodes, id);
        Ok(f(&mut client))
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let listen = args.get(1).map(String::as_str).unwrap_or("127.0.0.1:8080");
    let listener = TcpListener::bind(listen).expect("bind ui address");
    let state = Arc::new(AppState::new());
    println!("tessera ui on http://{listen}");

    for stream in listener.incoming() {
        let Ok(stream) = stream else {
            continue;
        };
        let state = state.clone();
        thread::spawn(move || handle_connection(stream, state));
    }
}

fn handle_connection(mut stream: TcpStream, state: Arc<AppState>) {
    let mut buf = [0u8; 64 * 1024];
    let Ok(read) = stream.read(&mut buf) else {
        return;
    };
    if read == 0 {
        return;
    }

    let mut request = buf[..read].to_vec();
    let Some(header_end) = find_header_end(&request) else {
        respond(&mut stream, 400, "text/plain", "bad request");
        return;
    };
    let head = String::from_utf8_lossy(&request[..header_end]).to_string();
    let content_length = content_length(&head);
    let body_start = header_end + 4;
    let current_body_len = request.len().saturating_sub(body_start);
    if current_body_len < content_length {
        let mut rest = vec![0u8; content_length - current_body_len];
        if stream.read_exact(&mut rest).is_err() {
            respond(&mut stream, 400, "text/plain", "bad request");
            return;
        }
        request.extend_from_slice(&rest);
    }
    let body_end = body_start + content_length;
    let body = String::from_utf8_lossy(&request[body_start..body_end]);
    let mut lines = head.lines();
    let Some(request_line) = lines.next() else {
        respond(&mut stream, 400, "text/plain", "bad request");
        return;
    };
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        respond(&mut stream, 400, "text/plain", "bad request");
        return;
    }
    let method = parts[0];
    let path = parts[1];

    match (method, path) {
        ("GET", "/") => respond(&mut stream, 200, "text/html; charset=utf-8", INDEX_HTML),
        ("GET", "/app.css") => respond(&mut stream, 200, "text/css; charset=utf-8", APP_CSS),
        ("GET", "/app.js") => respond(
            &mut stream,
            200,
            "application/javascript; charset=utf-8",
            APP_JS,
        ),
        ("POST", "/api/put") => handle_put(&mut stream, &state, &body),
        ("POST", "/api/get") => handle_get(&mut stream, &state, &body),
        ("POST", "/api/delete") => handle_delete(&mut stream, &state, &body),
        _ => respond(&mut stream, 404, "text/plain", "not found"),
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(head: &str) -> usize {
    head.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn handle_put(stream: &mut TcpStream, state: &AppState, body: &str) {
    let form = parse_form(body);
    let Ok(nodes) = parse_nodes(form.get("nodes").map(String::as_str).unwrap_or("")) else {
        respond_json(stream, 400, r#"{"ok":false,"error":"invalid nodes"}"#);
        return;
    };
    let key = form.get("key").cloned().unwrap_or_default();
    let value = form.get("value").cloned().unwrap_or_default();
    if key.is_empty() {
        respond_json(stream, 400, r#"{"ok":false,"error":"key is required"}"#);
        return;
    }

    let start = Instant::now();
    match state.with_client(nodes, |client| {
        client.put(key.as_bytes(), value.as_bytes());
        client.leader_hint()
    }) {
        Ok(leader) => respond_json(
            stream,
            200,
            &format!(
                r#"{{"ok":true,"operation":"put","leader":{},"elapsedMs":{}}}"#,
                leader,
                start.elapsed().as_millis()
            ),
        ),
        Err(err) => respond_json(stream, 500, &json_error(&err)),
    }
}

fn handle_get(stream: &mut TcpStream, state: &AppState, body: &str) {
    let form = parse_form(body);
    let Ok(nodes) = parse_nodes(form.get("nodes").map(String::as_str).unwrap_or("")) else {
        respond_json(stream, 400, r#"{"ok":false,"error":"invalid nodes"}"#);
        return;
    };
    let key = form.get("key").cloned().unwrap_or_default();
    if key.is_empty() {
        respond_json(stream, 400, r#"{"ok":false,"error":"key is required"}"#);
        return;
    }

    let start = Instant::now();
    match state.with_client(nodes, |client| {
        let value = client.get(key.as_bytes());
        (client.leader_hint(), value)
    }) {
        Ok((leader, value)) => {
            let value_json = value
                .map(|bytes| format!(r#""{}""#, escape_json(&String::from_utf8_lossy(&bytes))))
                .unwrap_or_else(|| "null".to_string());
            respond_json(
                stream,
                200,
                &format!(
                    r#"{{"ok":true,"operation":"get","leader":{},"value":{},"elapsedMs":{}}}"#,
                    leader,
                    value_json,
                    start.elapsed().as_millis()
                ),
            );
        }
        Err(err) => respond_json(stream, 500, &json_error(&err)),
    }
}

fn handle_delete(stream: &mut TcpStream, state: &AppState, body: &str) {
    let form = parse_form(body);
    let Ok(nodes) = parse_nodes(form.get("nodes").map(String::as_str).unwrap_or("")) else {
        respond_json(stream, 400, r#"{"ok":false,"error":"invalid nodes"}"#);
        return;
    };
    let key = form.get("key").cloned().unwrap_or_default();
    if key.is_empty() {
        respond_json(stream, 400, r#"{"ok":false,"error":"key is required"}"#);
        return;
    }

    let start = Instant::now();
    match state.with_client(nodes, |client| {
        client.delete(key.as_bytes());
        client.leader_hint()
    }) {
        Ok(leader) => respond_json(
            stream,
            200,
            &format!(
                r#"{{"ok":true,"operation":"delete","leader":{},"elapsedMs":{}}}"#,
                leader,
                start.elapsed().as_millis()
            ),
        ),
        Err(err) => respond_json(stream, 500, &json_error(&err)),
    }
}

fn parse_nodes(raw: &str) -> Result<Vec<SocketAddr>, String> {
    let nodes: Result<Vec<_>, _> = raw
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(str::parse)
        .collect();
    let nodes = nodes.map_err(|_| "could not parse node address")?;
    if nodes.is_empty() {
        return Err("at least one node is required".to_string());
    }
    Ok(nodes)
}

fn parse_form(body: &str) -> BTreeMap<String, String> {
    body.split('&')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some((percent_decode(key), percent_decode(value)))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let Ok(hex) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(hex);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn json_error(err: &str) -> String {
    format!(r#"{{"ok":false,"error":"{}"}}"#, escape_json(err))
}

fn escape_json(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect(),
            '\n' => "\\n".chars().collect(),
            '\r' => "\\r".chars().collect(),
            '\t' => "\\t".chars().collect(),
            c => vec![c],
        })
        .collect()
}

fn respond_json(stream: &mut TcpStream, status: u16, body: &str) {
    respond(stream, status, "application/json; charset=utf-8", body);
}

fn respond(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}
