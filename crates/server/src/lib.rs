use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration as Wall, Instant};

use consensus::{
    decode_message, decode_value, encode_delete, encode_get, encode_message, encode_put,
    ClientResult, Message, Raft, StateMachine,
};
use sim::{Action, Io, NodeId, Process, Rng, Time, TimerId};

mod store;
pub use store::DiskStore;

type Outbox = Sender<Vec<u8>>;
type Clients = Arc<Mutex<BTreeMap<NodeId, Outbox>>>;
type Inbox = Sender<(NodeId, Message)>;

const PEER: u8 = 0;
const CLIENT: u8 = 1;

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    stream.write_all(&len.to_le_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

fn handshake(role: u8, id: NodeId) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(role);
    frame.extend_from_slice(&(id as u64).to_le_bytes());
    frame
}

fn parse_handshake(frame: &[u8]) -> Option<(u8, NodeId)> {
    if frame.len() != 9 {
        return None;
    }
    let id = u64::from_le_bytes(frame[1..9].try_into().ok()?) as NodeId;
    Some((frame[0], id))
}

fn peer_writer(
    self_id: NodeId,
    addr: SocketAddr,
    rx: Receiver<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
) {
    'reconnect: while !shutdown.load(Ordering::Relaxed) {
        let mut stream = match TcpStream::connect(addr) {
            Ok(stream) => stream,
            Err(_) => {
                thread::sleep(Wall::from_millis(50));
                continue;
            }
        };
        if write_frame(&mut stream, &handshake(PEER, self_id)).is_err() {
            continue;
        }
        loop {
            let frame = match rx.recv() {
                Ok(frame) => frame,
                Err(_) => return,
            };
            if write_frame(&mut stream, &frame).is_err() {
                continue 'reconnect;
            }
        }
    }
}

fn reader(mut stream: TcpStream, from: NodeId, inbox: Inbox, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        match read_frame(&mut stream) {
            Ok(bytes) => match decode_message(&bytes) {
                Some(msg) => {
                    if inbox.send((from, msg)).is_err() {
                        return;
                    }
                }
                None => return,
            },
            Err(_) => return,
        }
    }
}

fn client_writer(mut stream: TcpStream, rx: Receiver<Vec<u8>>) {
    while let Ok(frame) = rx.recv() {
        if write_frame(&mut stream, &frame).is_err() {
            return;
        }
    }
}

fn accept_loop(addr: SocketAddr, inbox: Inbox, clients: Clients, shutdown: Arc<AtomicBool>) {
    let listener = match TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(_) => return,
    };
    let _ = listener.set_nonblocking(true);
    while !shutdown.load(Ordering::Relaxed) {
        let mut stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Wall::from_millis(5));
                continue;
            }
            Err(_) => continue,
        };
        let _ = stream.set_nonblocking(false);
        let Ok(frame) = read_frame(&mut stream) else {
            continue;
        };
        let Some((role, id)) = parse_handshake(&frame) else {
            continue;
        };
        if role == PEER {
            thread::spawn({
                let inbox = inbox.clone();
                let shutdown = shutdown.clone();
                move || reader(stream, id, inbox, shutdown)
            });
        } else {
            let Ok(writer) = stream.try_clone() else {
                continue;
            };
            let (tx, rx) = mpsc::channel();
            clients.lock().unwrap().insert(id, tx);
            thread::spawn(move || client_writer(writer, rx));
            thread::spawn({
                let inbox = inbox.clone();
                let shutdown = shutdown.clone();
                let clients = clients.clone();
                move || {
                    reader(stream, id, inbox, shutdown);
                    clients.lock().unwrap().remove(&id);
                }
            });
        }
    }
}

enum Cb {
    Start,
    Msg(NodeId, Message),
    Timer(TimerId),
}

struct Driver<SM: StateMachine> {
    id: NodeId,
    start: Instant,
    rng: Rng,
    raft: Raft<SM>,
    peer_tx: BTreeMap<NodeId, Outbox>,
    clients: Clients,
    timers: BTreeMap<TimerId, Instant>,
}

impl<SM: StateMachine> Driver<SM> {
    fn step(&mut self, event: Cb) {
        let now = Time(self.start.elapsed().as_nanos() as u64);
        let mut io = Io::new(self.id, now, &mut self.rng);
        match event {
            Cb::Start => self.raft.on_start(&mut io),
            Cb::Msg(from, msg) => self.raft.on_message(from, msg, &mut io),
            Cb::Timer(timer) => self.raft.on_timer(timer, &mut io),
        }
        for action in io.into_actions() {
            match action {
                Action::Send { to, msg } => {
                    let bytes = encode_message(&msg);
                    if let Some(tx) = self.peer_tx.get(&to) {
                        let _ = tx.send(bytes);
                    } else if let Some(tx) = self.clients.lock().unwrap().get(&to) {
                        let _ = tx.send(bytes);
                    }
                }
                Action::SetTimer { id, after } => {
                    let at = Instant::now() + Wall::from_nanos(after.as_nanos());
                    self.timers.insert(id, at);
                }
                Action::CancelTimer { id } => {
                    self.timers.remove(&id);
                }
            }
        }
    }

    fn next_timeout(&self) -> Wall {
        let now = Instant::now();
        self.timers
            .values()
            .min()
            .map(|&at| at.saturating_duration_since(now))
            .unwrap_or(Wall::from_millis(100))
    }

    fn fire_due(&mut self) {
        let now = Instant::now();
        let due: Vec<TimerId> = self
            .timers
            .iter()
            .filter(|&(_, &at)| at <= now)
            .map(|(&id, _)| id)
            .collect();
        for timer in due {
            self.timers.remove(&timer);
            self.step(Cb::Timer(timer));
        }
    }
}

pub fn run_node<SM: StateMachine>(
    id: NodeId,
    addr: SocketAddr,
    peers: BTreeMap<NodeId, SocketAddr>,
    raft: Raft<SM>,
    shutdown: Arc<AtomicBool>,
) {
    let (inbox_tx, inbox_rx) = mpsc::channel::<(NodeId, Message)>();
    let clients: Clients = Arc::new(Mutex::new(BTreeMap::new()));

    let mut peer_tx = BTreeMap::new();
    for (&pid, &paddr) in &peers {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        peer_tx.insert(pid, tx);
        let shutdown = shutdown.clone();
        thread::spawn(move || peer_writer(id, paddr, rx, shutdown));
    }

    thread::spawn({
        let inbox = inbox_tx.clone();
        let clients = clients.clone();
        let shutdown = shutdown.clone();
        move || accept_loop(addr, inbox, clients, shutdown)
    });
    drop(inbox_tx);

    let mut driver = Driver {
        id,
        start: Instant::now(),
        rng: Rng::new(0x7e55_e2a0_1234_0000 ^ id as u64),
        raft,
        peer_tx,
        clients,
        timers: BTreeMap::new(),
    };
    driver.step(Cb::Start);

    while !shutdown.load(Ordering::Relaxed) {
        match inbox_rx.recv_timeout(driver.next_timeout()) {
            Ok((from, msg)) => driver.step(Cb::Msg(from, msg)),
            Err(RecvTimeoutError::Timeout) => driver.fire_due(),
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

pub struct Client {
    nodes: Vec<SocketAddr>,
    id: NodeId,
    target: usize,
    stream: Option<TcpStream>,
    next_request: u64,
}

impl Client {
    pub fn new(nodes: Vec<SocketAddr>, id: NodeId) -> Self {
        Client {
            nodes,
            id,
            target: 0,
            stream: None,
            next_request: 1,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.request(encode_put(key, value));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.request(encode_delete(key));
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        decode_value(&self.request(encode_get(key)))
    }

    pub fn leader_hint(&self) -> NodeId {
        self.target
    }

    fn rotate(&mut self) {
        self.target = (self.target + 1) % self.nodes.len();
        self.stream = None;
    }

    fn dial(&mut self) -> bool {
        let mut stream = match TcpStream::connect(self.nodes[self.target]) {
            Ok(stream) => stream,
            Err(_) => return false,
        };
        if write_frame(&mut stream, &handshake(CLIENT, self.id)).is_err() {
            return false;
        }
        let _ = stream.set_read_timeout(Some(Wall::from_millis(500)));
        self.stream = Some(stream);
        true
    }

    fn request(&mut self, command: Vec<u8>) -> Vec<u8> {
        let request_id = self.next_request;
        self.next_request += 1;
        let msg = encode_message(&Message::ClientRequest {
            request_id,
            command,
        });
        loop {
            if self.stream.is_none() && !self.dial() {
                thread::sleep(Wall::from_millis(50));
                self.rotate();
                continue;
            }
            let mut stream = self.stream.take().unwrap();
            if write_frame(&mut stream, &msg).is_err() {
                self.rotate();
                continue;
            }
            let reply = read_frame(&mut stream)
                .ok()
                .and_then(|b| decode_message(&b));
            match reply {
                Some(Message::ClientReply {
                    request_id: rid,
                    result,
                }) if rid == request_id => match result {
                    ClientResult::Ok(value) => {
                        self.stream = Some(stream);
                        return value;
                    }
                    ClientResult::NotLeader(Some(hint)) if hint < self.nodes.len() => {
                        self.target = hint;
                    }
                    ClientResult::NotLeader(_) => self.rotate(),
                },
                _ => {
                    self.rotate();
                    thread::sleep(Wall::from_millis(50));
                }
            }
        }
    }
}
