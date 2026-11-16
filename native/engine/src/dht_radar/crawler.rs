//! DHT 爬虫：一个真实的 Mainline DHT 节点。
//!
//! 被动面：应答 `ping` / `find_node` / `get_peers` / `announce_peer`，从别人的
//! `get_peers` 与 `announce_peer` 里抄下 infohash 与来源 peer——这正是 BitComet
//! 「DHT 种子」列表的来源。
//! 主动面：持续 `find_node` 扩大节点表，让更多节点知道我们；对已知 infohash 发
//! `get_peers` 换取更多 peer 地址（元数据抓取要用）。
//!
//! 单线程阻塞循环，超时驱动，不依赖异步运行时。

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use rand::RngCore;

use super::krpc::{self, MessageKind, NodeId};
use super::store::{InfoHash, Store};
use super::{RadarStats, now_unix};

/// 已知的 DHT 引导节点。
const BOOTSTRAP_HOSTS: &[(&str, u16)] = &[
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.libtorrent.org", 25401),
];

/// 节点表上限。
const MAX_NODES: usize = 2048;
/// 应答 `nodes` 时返回的节点数。
const REPLY_NODES: usize = 8;
/// 每次维护向多少个节点发 `find_node`。
const EXPAND_PER_TICK: usize = 6;
/// 每次维护向多少个节点发 `get_peers`（针对已知 infohash）。
const SAMPLE_PER_TICK: usize = 20;
/// 每次维护向多少个节点发 `sample_infohashes`（BEP 51）——主动索引的主要来源。
const SAMPLE_HASHES_PER_TICK: usize = 3;
/// 为单个 infohash 找 peer 时，问几个距离最近的节点。
const GET_PEERS_FANOUT: usize = 4;
/// 应答别人 `sample_infohashes` 时最多返回的样本数。
const SAMPLE_HASHES_REPLY: usize = 50;
/// 单次读取的报文上限。
const RECV_BUFFER: usize = 2048;

/// 未完成查询的类型。
enum Pending {
    /// `get_peers`：应答里的 `values` 属于该 infohash。
    Peers(InfoHash),
    /// `sample_infohashes`：应答里的 `samples` 全部是新 infohash。
    SampleHashes,
}

/// 一个爬虫节点。
pub struct Crawler {
    socket: UdpSocket,
    node_id: NodeId,
    secret: [u8; 16],
    table: Vec<(NodeId, SocketAddrV4)>,
    /// 未完成的事务：应答回来时据此判断 `values` / `samples` 归属。
    pending: std::collections::HashMap<u16, Pending>,
    store: Arc<Store>,
    stats: Arc<RadarStats>,
}

impl Crawler {
    /// 绑定 UDP 端口；`port` 为 0 时由系统分配。
    pub fn bind(port: u16, store: Arc<Store>, stats: Arc<RadarStats>) -> std::io::Result<Self> {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port))?;
        socket.set_read_timeout(Some(Duration::from_millis(200)))?;
        let mut secret = [0u8; 16];
        rand::rng().fill_bytes(&mut secret);
        let bound = socket.local_addr()?.port();
        stats.bound_port.store(u32::from(bound), Ordering::Relaxed);
        Ok(Self {
            socket,
            node_id: krpc::random_node_id(),
            secret,
            table: Vec::new(),
            pending: std::collections::HashMap::new(),
            store,
            stats,
        })
    }

    /// 实际绑定的端口（用于日志与统计）。
    pub fn local_port(&self) -> Option<u16> {
        self.socket.local_addr().ok().map(|address| address.port())
    }

    /// 主循环；`stop` 置位后返回。
    pub fn run(mut self, stop: Arc<std::sync::atomic::AtomicBool>) {
        self.bootstrap();
        let mut buffer = [0u8; RECV_BUFFER];
        let mut last_maintenance = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            match self.socket.recv_from(&mut buffer) {
                Ok((size, from)) => {
                    self.stats.packets.fetch_add(1, Ordering::Relaxed);
                    self.stats.last_packet.store(now_unix(), Ordering::Relaxed);
                    self.handle(&buffer[..size], from);
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => break,
            }
            if last_maintenance.elapsed() >= Duration::from_secs(2) {
                last_maintenance = Instant::now();
                self.maintain();
            }
        }
    }

    /// 解析引导节点并写入节点表。
    fn bootstrap(&mut self) {
        for &(host, port) in BOOTSTRAP_HOSTS {
            let Ok(addresses) = (host, port).to_socket_addrs() else {
                continue;
            };
            for address in addresses {
                let SocketAddr::V4(address) = address else {
                    continue;
                };
                if let Err(error) = self.send_find_node(address, &krpc::random_node_id()) {
                    tracing::warn!(%address, %error, "dht-radar bootstrap query failed");
                }
            }
        }
        self.stats
            .nodes_known
            .store(self.table.len() as u64, Ordering::Relaxed);
    }

    /// 周期性维护：扩大节点表、为已知 infohash 找更多 peer。
    fn maintain(&mut self) {
        if self.table.is_empty() {
            self.bootstrap();
            return;
        }
        for _ in 0..EXPAND_PER_TICK {
            let index = self.pick_index();
            let (_, address) = self.table[index];
            if let Err(error) = self.send_find_node(address, &krpc::random_node_id()) {
                tracing::debug!(%error, "dht-radar find_node send failed");
            }
        }
        // BEP 51 只给 infohash、不给 peer。对还没有 peer 的条目做一次小范围查找：
        // 问节点表里距离该 infohash 最近的几个节点，命中率远高于随机节点。
        for infohash in self.store.peerless_pending(SAMPLE_PER_TICK, now_unix()) {
            for address in self.closest_addresses(&infohash, GET_PEERS_FANOUT) {
                if let Err(error) = self.send_get_peers(address, &infohash) {
                    tracing::debug!(%error, "dht-radar get_peers send failed");
                }
            }
        }
        // BEP 51：向节点索取它索引的 infohash 样本，这是「没被查询」时唯一的主动来源。
        for _ in 0..SAMPLE_HASHES_PER_TICK {
            let index = self.pick_index();
            let (_, address) = self.table[index];
            if let Err(error) = self.send_sample_infohashes(address) {
                tracing::debug!(%error, "dht-radar sample_infohashes send failed");
            }
        }
        self.stats
            .nodes_known
            .store(self.table.len() as u64, Ordering::Relaxed);
    }

    fn pick_index(&self) -> usize {
        let mut bytes = [0u8; 8];
        rand::rng().fill_bytes(&mut bytes);
        (u64::from_le_bytes(bytes) as usize) % self.table.len()
    }

    fn send_to(&self, address: SocketAddrV4, payload: &[u8]) -> std::io::Result<()> {
        self.socket.send_to(payload, address)?;
        self.stats.packets_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn send_find_node(&self, address: SocketAddrV4, target: &NodeId) -> std::io::Result<()> {
        let args = krpc::Value::dict([
            (&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec())),
            (&b"target"[..], krpc::Value::Bytes(target.to_vec())),
        ]);
        let payload = krpc::query(self.next_tid(), "find_node", args);
        self.send_to(address, &payload)
    }

    fn send_get_peers(
        &mut self,
        address: SocketAddrV4,
        infohash: &InfoHash,
    ) -> std::io::Result<()> {
        let transaction = self.next_tid();
        if self.pending.len() >= 512 {
            self.pending.clear();
        }
        self.pending.insert(transaction, Pending::Peers(*infohash));
        let args = krpc::Value::dict([
            (&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec())),
            (&b"info_hash"[..], krpc::Value::Bytes(infohash.to_vec())),
        ]);
        let payload = krpc::query(transaction, "get_peers", args);
        self.send_to(address, &payload)
    }

    /// BEP 51：索取节点索引的 infohash 样本。
    fn send_sample_infohashes(&mut self, address: SocketAddrV4) -> std::io::Result<()> {
        let transaction = self.next_tid();
        if self.pending.len() >= 512 {
            self.pending.clear();
        }
        self.pending.insert(transaction, Pending::SampleHashes);
        let target = krpc::random_node_id();
        let args = krpc::Value::dict([
            (&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec())),
            (&b"target"[..], krpc::Value::Bytes(target.to_vec())),
        ]);
        let payload = krpc::query(transaction, "sample_infohashes", args);
        self.send_to(address, &payload)
    }

    fn next_tid(&self) -> u16 {
        let mut bytes = [0u8; 2];
        rand::rng().fill_bytes(&mut bytes);
        u16::from_be_bytes(bytes)
    }

    /// 处理一条报文。
    fn handle(&mut self, payload: &[u8], from: SocketAddr) {
        let Ok(message) = krpc::parse(payload) else {
            return;
        };
        match krpc::kind(&message) {
            Some(MessageKind::Query) => self.handle_query(&message, from),
            Some(MessageKind::Response) => self.handle_response(&message, from),
            Some(MessageKind::Error) | None => {}
        }
    }

    fn handle_query(&mut self, message: &krpc::Value, from: SocketAddr) {
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        let Some(transaction) = krpc::transaction(message) else {
            return;
        };
        let Some(name) = krpc::query_name(message) else {
            return;
        };
        let empty = krpc::Value::Dict(Default::default());
        let args = krpc::query_args(message).unwrap_or(&empty);
        if let Some(id) = krpc::sender_id(args) {
            self.remember(id, from);
        }

        match name {
            "ping" => {
                let body =
                    krpc::Value::dict([(&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec()))]);
                self.reply(transaction, body, from);
            }
            "find_node" => {
                let target: NodeId = args
                    .get_bytes(b"target")
                    .and_then(|bytes| bytes.try_into().ok())
                    .unwrap_or(self.node_id);
                let body = krpc::Value::dict([
                    (&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec())),
                    (
                        &b"nodes"[..],
                        krpc::Value::Bytes(self.closest_nodes(&target)),
                    ),
                ]);
                self.reply(transaction, body, from);
            }
            "get_peers" => {
                let Some(infohash) = args
                    .get_bytes(b"info_hash")
                    .and_then(|bytes| <[u8; 20]>::try_from(bytes).ok())
                else {
                    self.reply_error(transaction, 203, "bad info_hash", from);
                    return;
                };
                self.harvest(infohash, Some(from));
                let body = krpc::Value::dict([
                    (&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec())),
                    (
                        &b"token"[..],
                        krpc::Value::Bytes(krpc::token_for(&self.secret, from)),
                    ),
                    (
                        &b"nodes"[..],
                        krpc::Value::Bytes(self.closest_nodes(&infohash)),
                    ),
                ]);
                self.reply(transaction, body, from);
            }
            "announce_peer" => {
                let Some(infohash) = args
                    .get_bytes(b"info_hash")
                    .and_then(|bytes| <[u8; 20]>::try_from(bytes).ok())
                else {
                    self.reply_error(transaction, 203, "bad info_hash", from);
                    return;
                };
                // `implied_port` 为真时用报文源端口；否则用 `port`。
                let implied = args.get_int(b"implied_port").unwrap_or(0) != 0;
                let port = args
                    .get_int(b"port")
                    .and_then(|value| u16::try_from(value).ok())
                    .filter(|value| *value > 0);
                let peer = match (implied, port) {
                    (true, _) | (false, None) => Some(from),
                    (false, Some(port)) => Some(SocketAddr::new(from.ip(), port)),
                };
                self.harvest(infohash, peer);
                let body =
                    krpc::Value::dict([(&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec()))]);
                self.reply(transaction, body, from);
            }
            "sample_infohashes" => {
                let samples = self.store.sample(SAMPLE_HASHES_REPLY);
                let mut packed = Vec::with_capacity(samples.len() * 20);
                for hash in &samples {
                    packed.extend_from_slice(hash);
                }
                let total = i64::try_from(self.store.stats().torrents).unwrap_or(i64::MAX);
                let body = krpc::Value::dict([
                    (&b"id"[..], krpc::Value::Bytes(self.node_id.to_vec())),
                    (&b"interval"[..], krpc::Value::Int(300)),
                    (&b"num"[..], krpc::Value::Int(total)),
                    (&b"samples"[..], krpc::Value::Bytes(packed)),
                    (
                        &b"nodes"[..],
                        krpc::Value::Bytes(self.closest_nodes(&self.node_id)),
                    ),
                ]);
                self.reply(transaction, body, from);
            }
            _ => self.reply_error(transaction, 204, "method unknown", from),
        }
    }

    fn handle_response(&mut self, message: &krpc::Value, from: SocketAddr) {
        self.stats.responses.fetch_add(1, Ordering::Relaxed);
        let Some(body) = krpc::response_body(message) else {
            return;
        };
        if let Some(id) = body
            .get_bytes(b"id")
            .and_then(|bytes| <[u8; 20]>::try_from(bytes).ok())
        {
            self.remember(id, from);
        }
        if let Some(nodes) = body.get_bytes(b"nodes") {
            for (id, address) in krpc::decode_nodes(nodes) {
                self.remember(id, SocketAddr::V4(address));
            }
        }
        // 按事务 ID 归属：`get_peers` 的 `values` 属于该 infohash，
        // `sample_infohashes` 的 `samples` 是节点索引的一批 infohash（BEP 51）。
        let Some(transaction) = krpc::transaction(message) else {
            return;
        };
        let Some(pending) = self.pending.remove(&transaction) else {
            return;
        };
        match pending {
            Pending::Peers(infohash) => {
                for peer in krpc::decode_peers(body) {
                    self.harvest(infohash, Some(SocketAddr::V4(peer)));
                }
            }
            Pending::SampleHashes => {
                if let Some(samples) = body.get_bytes(b"samples") {
                    for infohash in krpc::decode_samples(samples) {
                        self.harvest(infohash, None);
                    }
                }
            }
        }
    }

    /// 记下一个 infohash 观测。私网 / 环回地址不是可连的 peer，直接丢掉。
    fn harvest(&mut self, infohash: InfoHash, peer: Option<SocketAddr>) {
        let peer = peer.filter(|address| is_global(*address));
        let (is_new, _) = self.store.observe(infohash, peer, now_unix());
        if is_new {
            self.stats.infohashes.fetch_add(1, Ordering::Relaxed);
        }
        if peer.is_some() {
            self.stats.peers.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 写入节点表（按地址去重，满了随机替换）。
    fn remember(&mut self, id: NodeId, from: SocketAddr) {
        let SocketAddr::V4(address) = from else {
            return;
        };
        if address.port() == 0 || address.ip().is_unspecified() {
            return;
        }
        if self.table.iter().any(|(_, known)| *known == address) {
            return;
        }
        if self.table.len() >= MAX_NODES {
            let index = self.pick_index();
            self.table[index] = (id, address);
            return;
        }
        self.table.push((id, address));
    }

    /// 与 `target` 异或距离最近的若干节点地址。
    fn closest_addresses(&self, target: &NodeId, count: usize) -> Vec<SocketAddrV4> {
        let mut scored: Vec<(u8, SocketAddrV4)> = self
            .table
            .iter()
            .map(|node| (xor_leading(&node.0, target), node.1))
            .collect();
        scored.sort_by_key(|(distance, _)| *distance);
        scored
            .into_iter()
            .rev()
            .take(count)
            .map(|(_, address)| address)
            .collect()
    }

    /// 与 `target` 异或距离最近的若干节点，紧凑编码。
    fn closest_nodes(&self, target: &NodeId) -> Vec<u8> {
        let mut scored: Vec<(u8, &(NodeId, SocketAddrV4))> = self
            .table
            .iter()
            .map(|node| (xor_leading(&node.0, target), node))
            .collect();
        scored.sort_by_key(|(distance, _)| *distance);
        let nodes: Vec<(NodeId, SocketAddrV4)> = scored
            .into_iter()
            .take(REPLY_NODES)
            .map(|(_, node)| *node)
            .collect();
        krpc::encode_nodes(&nodes)
    }

    fn reply(&self, transaction: u16, body: krpc::Value, to: SocketAddr) {
        let payload = krpc::response(transaction, body);
        if let Err(error) = self.socket.send_to(&payload, to) {
            tracing::debug!(%error, "dht-radar reply send failed");
        }
        self.stats.packets_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn reply_error(&self, transaction: u16, code: i64, message: &str, to: SocketAddr) {
        let payload = krpc::error_response(transaction, code, message);
        if let Err(error) = self.socket.send_to(&payload, to) {
            tracing::debug!(%error, "dht-radar error reply send failed");
        }
        self.stats.packets_sent.fetch_add(1, Ordering::Relaxed);
    }
}

/// 是否是公网可连地址。
fn is_global(address: SocketAddr) -> bool {
    match address.ip() {
        std::net::IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.is_documentation())
        }
        std::net::IpAddr::V6(ip) => !(ip.is_loopback() || ip.is_unspecified() || ip.is_multicast()),
    }
}

/// 异或距离的前导零字节数：越大越近，用于粗排序（够用且便宜）。
fn xor_leading(left: &NodeId, right: &NodeId) -> u8 {
    let mut count = 0;
    for (a, b) in left.iter().zip(right.iter()) {
        let mixed = a ^ b;
        if mixed == 0 {
            count += 1;
            continue;
        }
        break;
    }
    count
}
