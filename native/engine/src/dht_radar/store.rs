//! 采集结果聚合：按 infohash 去重、累计 peer、记录元数据状态，并按容量上限淘汰。
//!
//! 全部状态在一把互斥锁后面：写入来自采集线程（每秒几十次），读取来自 RPC（每秒几次），
//! 临界区只做哈希表操作，不涉及 IO。

use std::collections::hash_map::Entry as HashEntry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};

/// infohash。
pub type InfoHash = [u8; 20];

/// 元数据抓取状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataState {
    /// 还没有元数据，也没有试过。
    Pending,
    /// 正在向 peer 拉取。
    Fetching,
    /// 已拿到并通过 SHA-1 校验。
    Ready,
    /// 多次尝试失败，暂不再试。
    Failed,
}

impl MetadataState {
    /// 是否还值得再试一次。
    pub fn retryable(self) -> bool {
        matches!(self, Self::Pending | Self::Failed)
    }
}

/// 一条聚合后的种子。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentEntry {
    /// infohash。
    pub infohash: InfoHash,
    /// 种子名（元数据里的 `name`）。
    pub name: Option<String>,
    /// 总字节数（元数据里的 `length` 或 `files` 求和）。
    pub total_size: Option<u64>,
    /// 文件数（单文件种子为 1）。
    pub file_count: Option<u32>,
    /// 观测到的不同 peer 地址数（上限见 [`PEERS_PER_TORRENT`]）。
    pub peers: u32,
    /// 首次观测时间（Unix 秒）。
    pub first_seen: i64,
    /// 最近观测时间（Unix 秒）。
    pub last_seen: i64,
    /// 元数据状态。
    pub metadata: MetadataState,
}

impl TorrentEntry {
    /// infohash 的十六进制小写形式（磁力链接与展示都用它）。
    pub fn info_hash_hex(&self) -> String {
        hex(&self.infohash)
    }

    /// 可直接交给下载引擎的磁力链接。
    pub fn magnet(&self) -> String {
        format!("magnet:?xt=urn:btih:{}", self.info_hash_hex())
    }
}

/// 取锁；被 panic 打断时取回内部值——雷达的聚合状态没有「半写坏」的不变量，
/// 继续用旧数据比让采集线程集体 panic 更安全。
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 单个种子最多记住的 peer 数：只用于挑一个可连的 peer 拉元数据。
const PEERS_PER_TORRENT: usize = 8;
/// 少于这个数量的 peer 就继续用 `get_peers` 补——随机 peer 列表里死地址很多。
const PEERS_MIN_FOR_METADATA: usize = 3;
/// 单条种子的元数据最多尝试次数（每次换一个 peer）。
const METADATA_MAX_ATTEMPTS: u8 = 5;

/// 排序方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    /// 最近观测优先（默认）。
    Recent,
    /// peer 数多的优先。
    Peers,
    /// 体积大的优先。
    Size,
}

impl Sort {
    /// 由请求字符串解析，未知值退回默认。
    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("peers") => Self::Peers,
            Some("size") => Self::Size,
            _ => Self::Recent,
        }
    }
}

/// 聚合统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// 当前保留的种子数。
    pub torrents: usize,
    /// 其中已有元数据的数量。
    pub with_metadata: usize,
    /// 当前保留的 peer 地址数（去重前）。
    pub peers: usize,
    /// 进程启动以来观测到的 infohash 总数（含被淘汰的）。
    pub observed: u64,
}

struct Stored {
    entry: TorrentEntry,
    peers: Vec<SocketAddr>,
    attempts: u8,
}

#[derive(Default)]
struct Inner {
    torrents: HashMap<InfoHash, Stored>,
    observed: u64,
}

/// 聚合仓库。
pub struct Store {
    inner: Mutex<Inner>,
    capacity: usize,
}

impl Store {
    /// 按容量上限创建。
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            capacity: capacity.max(16),
        }
    }

    /// 记录一次观测。返回「首次出现」与一个可用于拉元数据的 peer 地址。
    pub fn observe(
        &self,
        infohash: InfoHash,
        peer: Option<SocketAddr>,
        now: i64,
    ) -> (bool, Option<SocketAddr>) {
        let mut inner = lock(&self.inner);
        if !inner.torrents.contains_key(&infohash) {
            inner.observed = inner.observed.saturating_add(1);
        }
        let candidate = match inner.torrents.entry(infohash) {
            HashEntry::Occupied(mut occupied) => {
                let stored = occupied.get_mut();
                stored.entry.last_seen = now;
                if let Some(address) = peer
                    && !stored.peers.contains(&address)
                    && stored.peers.len() < PEERS_PER_TORRENT
                {
                    stored.peers.push(address);
                    stored.entry.peers = stored.peers.len() as u32;
                }
                (false, peer.filter(|address| stored.peers.contains(address)))
            }
            HashEntry::Vacant(vacant) => {
                let peers = peer.into_iter().collect::<Vec<_>>();
                vacant.insert(Stored {
                    entry: TorrentEntry {
                        infohash,
                        name: None,
                        total_size: None,
                        file_count: None,
                        peers: peers.len() as u32,
                        first_seen: now,
                        last_seen: now,
                        metadata: MetadataState::Pending,
                    },
                    peers: peers.clone(),
                    attempts: 0,
                });
                (true, peers.first().copied())
            }
        };
        if inner.torrents.len() > self.capacity {
            Self::evict(&mut inner, self.capacity);
        }
        candidate
    }

    /// 淘汰最久未观测的条目，直到回落到容量以内。
    fn evict(inner: &mut Inner, capacity: usize) {
        while inner.torrents.len() > capacity {
            let Some(oldest) = inner
                .torrents
                .iter()
                .min_by_key(|(_, stored)| stored.entry.last_seen)
                .map(|(hash, _)| *hash)
            else {
                return;
            };
            inner.torrents.remove(&oldest);
        }
    }

    /// 标记开始抓取元数据；返回 `false` 表示当前状态不需要抓取（已在抓或已就绪）。
    pub fn begin_metadata(&self, infohash: InfoHash) -> bool {
        let mut inner = lock(&self.inner);
        let Some(stored) = inner.torrents.get_mut(&infohash) else {
            return false;
        };
        if !stored.entry.metadata.retryable() || stored.attempts >= METADATA_MAX_ATTEMPTS {
            return false;
        }
        stored.entry.metadata = MetadataState::Fetching;
        stored.attempts = stored.attempts.saturating_add(1);
        true
    }

    /// 写入元数据摘要。
    pub fn set_metadata(&self, infohash: InfoHash, summary: MetadataSummary) {
        let mut inner = lock(&self.inner);
        if let Some(stored) = inner.torrents.get_mut(&infohash) {
            stored.entry.name = summary.name;
            stored.entry.total_size = summary.total_size;
            stored.entry.file_count = summary.file_count;
            stored.entry.metadata = MetadataState::Ready;
        }
    }

    /// 标记抓取失败（`retryable` 由 `begin_metadata` 的尝试次数决定）。
    pub fn fail_metadata(&self, infohash: InfoHash) {
        let mut inner = lock(&self.inner);
        if let Some(stored) = inner.torrents.get_mut(&infohash)
            && stored.entry.metadata == MetadataState::Fetching
        {
            stored.entry.metadata = MetadataState::Failed;
        }
    }

    /// 取一批待抓元数据的 `(infohash, peer)`。
    pub fn metadata_candidates(&self, limit: usize) -> Vec<(InfoHash, SocketAddr)> {
        let inner = lock(&self.inner);
        inner
            .torrents
            .values()
            .filter(|stored| {
                stored.entry.metadata.retryable()
                    && stored.attempts < METADATA_MAX_ATTEMPTS
                    && !stored.peers.is_empty()
            })
            .take(limit)
            .filter_map(|stored| {
                if stored.peers.is_empty() {
                    return None;
                }
                // 按已尝试次数轮换 peer：第一次连的往往已下线，重试必须换一个地址，
                // 否则每次都撞在同一具尸体上。
                let index = usize::from(stored.attempts) % stored.peers.len();
                stored
                    .peers
                    .get(index)
                    .map(|address| (stored.entry.infohash, *address))
            })
            .collect()
    }

    /// 查询一页结果。
    pub fn query(
        &self,
        needle: Option<&str>,
        sort: Sort,
        offset: usize,
        limit: usize,
    ) -> (Vec<TorrentEntry>, usize) {
        let inner = lock(&self.inner);
        let needle = needle
            .map(str::to_lowercase)
            .filter(|text| !text.is_empty());
        let mut items: Vec<TorrentEntry> = inner
            .torrents
            .values()
            .filter(|stored| match &needle {
                None => true,
                Some(text) => {
                    stored
                        .entry
                        .name
                        .as_deref()
                        .is_some_and(|name| name.to_lowercase().contains(text))
                        || hex(&stored.entry.infohash).contains(text)
                }
            })
            .map(|stored| stored.entry.clone())
            .collect();
        match sort {
            Sort::Recent => items.sort_by_key(|entry| std::cmp::Reverse(entry.last_seen)),
            Sort::Peers => items.sort_by(|a, b| {
                b.peers
                    .cmp(&a.peers)
                    .then_with(|| b.last_seen.cmp(&a.last_seen))
            }),
            Sort::Size => items.sort_by(|a, b| {
                b.total_size
                    .unwrap_or(0)
                    .cmp(&a.total_size.unwrap_or(0))
                    .then_with(|| b.last_seen.cmp(&a.last_seen))
            }),
        }
        let total = items.len();
        let page = items
            .into_iter()
            .skip(offset)
            .take(limit.clamp(1, 500))
            .collect();
        (page, total)
    }

    /// 当前统计。
    pub fn stats(&self) -> StoreStats {
        let inner = lock(&self.inner);
        StoreStats {
            torrents: inner.torrents.len(),
            with_metadata: inner
                .torrents
                .values()
                .filter(|stored| stored.entry.metadata == MetadataState::Ready)
                .count(),
            peers: inner
                .torrents
                .values()
                .map(|stored| stored.peers.len())
                .sum(),
            observed: inner.observed,
        }
    }

    /// 还没有 peer、仍值得查一次 `get_peers` 的 infohash（BEP 51 样本不带 peer 地址）。
    pub fn peerless_pending(&self, limit: usize) -> Vec<InfoHash> {
        let inner = lock(&self.inner);
        inner
            .torrents
            .values()
            .filter(|stored| {
                stored.peers.len() < PEERS_MIN_FOR_METADATA && stored.entry.metadata.retryable()
            })
            .take(limit)
            .map(|stored| stored.entry.infohash)
            .collect()
    }

    /// 取最多 `limit` 个 infohash，用于应答别人的 `sample_infohashes`（BEP 51）。
    pub fn sample(&self, limit: usize) -> Vec<InfoHash> {
        let inner = lock(&self.inner);
        inner.torrents.keys().take(limit).copied().collect()
    }

    /// 已有元数据的 infohash 集合，用于避免重复抓取。
    pub fn ready_hashes(&self) -> HashSet<InfoHash> {
        let inner = lock(&self.inner);
        inner
            .torrents
            .values()
            .filter(|stored| stored.entry.metadata == MetadataState::Ready)
            .map(|stored| stored.entry.infohash)
            .collect()
    }
}

/// 元数据摘要（`info` 字典里展示需要的字段）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSummary {
    /// 种子名。
    pub name: Option<String>,
    /// 总字节数。
    pub total_size: Option<u64>,
    /// 文件数。
    pub file_count: Option<u32>,
}

/// 20 字节摘要转小写十六进制。
pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: u8) -> InfoHash {
        [seed; 20]
    }

    fn peer(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn first_observation_reports_new_and_peer() {
        let store = Store::new(64);
        let (is_new, candidate) = store.observe(hash(1), Some(peer(6881)), 100);
        assert!(is_new);
        assert_eq!(candidate, Some(peer(6881)));
        assert_eq!(store.stats().torrents, 1);
        assert_eq!(store.stats().observed, 1);
    }

    #[test]
    fn repeated_observations_accumulate_peers_without_duplicates() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 100);
        store.observe(hash(1), Some(peer(2)), 101);
        store.observe(hash(1), Some(peer(1)), 102);
        let (page, total) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(total, 1);
        assert_eq!(page[0].peers, 2);
        assert_eq!(page[0].last_seen, 102);
        assert_eq!(store.stats().observed, 1);
    }

    #[test]
    fn peers_per_torrent_are_capped() {
        let store = Store::new(64);
        for port in 0..(PEERS_PER_TORRENT as u16 + 5) {
            store.observe(hash(1), Some(peer(port + 1)), 100);
        }
        let (page, _) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(page[0].peers, PEERS_PER_TORRENT as u32);
    }

    #[test]
    fn capacity_evicts_least_recently_seen() {
        let store = Store::new(16);
        for seed in 0..20u8 {
            store.observe(hash(seed), None, i64::from(seed));
        }
        let stats = store.stats();
        assert_eq!(stats.torrents, 16);
        assert_eq!(stats.observed, 20);
        let (page, total) = store.query(None, Sort::Recent, 0, 100);
        assert_eq!(total, 16);
        // 最新的条目一定还在，最旧的已被淘汰。
        assert!(page.iter().any(|entry| entry.infohash == hash(19)));
        assert!(!page.iter().any(|entry| entry.infohash == hash(0)));
    }

    #[test]
    fn metadata_state_machine_limits_attempts() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 100);
        assert!(store.begin_metadata(hash(1)));
        // 抓取中不再重复进入。
        assert!(!store.begin_metadata(hash(1)));
        store.fail_metadata(hash(1));
        for _ in 1..METADATA_MAX_ATTEMPTS {
            assert!(store.begin_metadata(hash(1)));
            store.fail_metadata(hash(1));
        }
        // 达到尝试上限后放弃。
        assert!(!store.begin_metadata(hash(1)));
        assert!(store.metadata_candidates(10).is_empty());
    }

    #[test]
    fn metadata_attempts_rotate_through_peers() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 100);
        store.observe(hash(1), Some(peer(2)), 100);
        let mut seen = Vec::new();
        for _ in 0..METADATA_MAX_ATTEMPTS {
            let candidates = store.metadata_candidates(1);
            assert_eq!(candidates.len(), 1);
            seen.push(candidates[0].1);
            assert!(store.begin_metadata(hash(1)));
            store.fail_metadata(hash(1));
        }
        // 每次重试换一个 peer，而不是一直撞同一具尸体。
        assert!(seen.contains(&peer(1)));
        assert!(seen.contains(&peer(2)));
    }

    #[test]
    fn metadata_summary_fills_entry() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 100);
        assert!(store.begin_metadata(hash(1)));
        store.set_metadata(
            hash(1),
            MetadataSummary {
                name: Some("ubuntu.iso".to_owned()),
                total_size: Some(1234),
                file_count: Some(1),
            },
        );
        let (page, _) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(page[0].name.as_deref(), Some("ubuntu.iso"));
        assert_eq!(page[0].total_size, Some(1234));
        assert_eq!(page[0].metadata, MetadataState::Ready);
        assert_eq!(store.stats().with_metadata, 1);
        assert!(store.ready_hashes().contains(&hash(1)));
    }

    #[test]
    fn search_matches_name_and_hash_prefix() {
        let store = Store::new(64);
        store.observe(hash(0xab), None, 100);
        store.set_metadata(
            hash(0xab),
            MetadataSummary {
                name: Some("Debian Netinst".to_owned()),
                total_size: None,
                file_count: None,
            },
        );
        assert_eq!(store.query(Some("debian"), Sort::Recent, 0, 10).1, 1);
        assert_eq!(store.query(Some("ababab"), Sort::Recent, 0, 10).1, 1);
        assert_eq!(store.query(Some("nothing"), Sort::Recent, 0, 10).1, 0);
    }

    #[test]
    fn sort_orders_by_requested_field() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 100);
        store.observe(hash(2), Some(peer(1)), 200);
        store.observe(hash(2), Some(peer(2)), 200);
        let (recent, _) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(recent[0].infohash, hash(2));
        let (by_peers, _) = store.query(None, Sort::Peers, 0, 10);
        assert_eq!(by_peers[0].infohash, hash(2));
        assert_eq!(by_peers[0].peers, 2);
    }

    #[test]
    fn pagination_slices_sorted_results() {
        let store = Store::new(64);
        for seed in 0..5u8 {
            store.observe(hash(seed), None, i64::from(seed));
        }
        let (first, total) = store.query(None, Sort::Recent, 0, 2);
        let (second, _) = store.query(None, Sort::Recent, 2, 2);
        assert_eq!(total, 5);
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_ne!(first[0].infohash, second[0].infohash);
    }

    #[test]
    fn magnet_uses_hex_infohash() {
        let entry = TorrentEntry {
            infohash: hash(0x0f),
            name: None,
            total_size: None,
            file_count: None,
            peers: 0,
            first_seen: 0,
            last_seen: 0,
            metadata: MetadataState::Pending,
        };
        assert_eq!(
            entry.magnet(),
            format!("magnet:?xt=urn:btih:{}", "0f".repeat(20))
        );
    }
}
