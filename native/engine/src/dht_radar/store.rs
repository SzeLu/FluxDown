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
/// DHT 里死地址比例很高，多存几个才能在一轮尝试里碰到活的。
const PEERS_PER_TORRENT: usize = 32;
/// 少于这个数量的 peer 就继续用 `get_peers` 补——随机 peer 列表里死地址很多。
const PEERS_MIN_FOR_METADATA: usize = 3;
/// 单条种子一轮元数据尝试次数（每次换一个 peer）。
const METADATA_MAX_ATTEMPTS: u8 = 5;
/// 一轮尝试全部失败后的冷却时间（秒）。到点后重新给一轮机会，
/// 这样长期运行能把「刚发布时还没几个 peer」的条目逐步补上名字。
const METADATA_COOLDOWN_SECS: i64 = 600;
/// 已落库的条目，最近观测时间最多每隔这么久刷新一次库里的 `last_seen`
/// （避免每次观测都排队写库）。
const PERSIST_TOUCH_SECS: i64 = 300;

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
    /// 冷却到什么时候才允许再试元数据（Unix 秒）。
    retry_after: i64,
    /// 上次排进持久化队列的时间（Unix 秒），用于给 `last_seen` 刷新节流。
    persisted_at: i64,
    /// 上次尝试抓元数据的时间（Unix 秒），用于轮转候选。
    last_attempt: i64,
}

#[derive(Default)]
struct Inner {
    torrents: HashMap<InfoHash, Stored>,
    observed: u64,
    /// 待写入数据库的条目（元数据解析成功、或已落库条目的观测时间需要刷新）。
    /// 用集合去重：同一条在刷库前被多次标记只写一次。
    pending_persist: HashSet<InfoHash>,
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
        let (is_new, candidate, touch_persist) = match inner.torrents.entry(infohash) {
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
                // 已解析出名字的条目要定期把 `last_seen` 同步进库，但不必每次观测都写。
                let touch = stored.entry.metadata == MetadataState::Ready
                    && now - stored.persisted_at >= PERSIST_TOUCH_SECS;
                if touch {
                    stored.persisted_at = now;
                }
                (
                    false,
                    peer.filter(|address| stored.peers.contains(address)),
                    touch,
                )
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
                    retry_after: 0,
                    persisted_at: 0,
                    last_attempt: 0,
                });
                (true, peers.first().copied(), false)
            }
        };
        if touch_persist {
            inner.pending_persist.insert(infohash);
        }
        if inner.torrents.len() > self.capacity {
            // 一次多淘汰 5%，避免「每观测一条就全表扫一遍」。
            let target = self.capacity - self.capacity / 20;
            Self::evict(&mut inner, target);
        }
        (is_new, candidate)
    }

    /// 淘汰最久未观测的条目，直到回落到容量以内。
    ///
    /// 优先丢「还没解析出名字」的：有名字的条目才是用户能直接用的，采集速度远高于
    /// 元数据解析速度，不做这个区分的话，列表会被纯 hash 条目挤满、名字刚解析出来就被淘汰。
    fn evict(inner: &mut Inner, capacity: usize) {
        while inner.torrents.len() > capacity {
            let mut anonymous: Option<(InfoHash, i64)> = None;
            let mut named: Option<(InfoHash, i64)> = None;
            for (hash, stored) in &inner.torrents {
                let slot = if stored.entry.metadata == MetadataState::Ready {
                    &mut named
                } else {
                    &mut anonymous
                };
                let better = match *slot {
                    Some((_, seen)) => stored.entry.last_seen < seen,
                    None => true,
                };
                if better {
                    *slot = Some((*hash, stored.entry.last_seen));
                }
            }
            let Some((victim, _)) = anonymous.or(named) else {
                return;
            };
            inner.torrents.remove(&victim);
        }
    }

    /// 标记开始抓取元数据；返回 `false` 表示当前状态不需要抓取（已在抓、已就绪或还在冷却）。
    pub fn begin_metadata(&self, infohash: InfoHash, now: i64) -> bool {
        let mut inner = lock(&self.inner);
        let Some(stored) = inner.torrents.get_mut(&infohash) else {
            return false;
        };
        if !stored.entry.metadata.retryable()
            || stored.attempts >= METADATA_MAX_ATTEMPTS
            || now < stored.retry_after
        {
            return false;
        }
        stored.entry.metadata = MetadataState::Fetching;
        stored.attempts = stored.attempts.saturating_add(1);
        stored.last_attempt = now;
        true
    }

    /// 写入元数据摘要，并排进持久化队列。
    pub fn set_metadata(&self, infohash: InfoHash, summary: MetadataSummary, now: i64) {
        let mut inner = lock(&self.inner);
        if let Some(stored) = inner.torrents.get_mut(&infohash) {
            stored.entry.name = summary.name;
            stored.entry.total_size = summary.total_size;
            stored.entry.file_count = summary.file_count;
            stored.entry.metadata = MetadataState::Ready;
            stored.attempts = 0;
            stored.retry_after = 0;
            stored.persisted_at = now;
            inner.pending_persist.insert(infohash);
        }
    }

    /// 标记抓取失败。一轮尝试用完后进入冷却，冷却到点会重新给一轮机会。
    pub fn fail_metadata(&self, infohash: InfoHash, now: i64) {
        let mut inner = lock(&self.inner);
        if let Some(stored) = inner.torrents.get_mut(&infohash)
            && stored.entry.metadata == MetadataState::Fetching
        {
            stored.entry.metadata = MetadataState::Failed;
            if stored.attempts >= METADATA_MAX_ATTEMPTS {
                stored.attempts = 0;
                stored.retry_after = now.saturating_add(METADATA_COOLDOWN_SECS);
            }
        }
    }

    /// 取一批待抓元数据的 `(infohash, peer)`，最久没尝试过的优先。
    ///
    /// 不能直接 `take(limit)`：`HashMap` 的迭代顺序在一张不变的表上是稳定的，
    /// 多个抓取线程会一直抢同一小批条目，新采到的条目永远轮不到，而且失败重试
    /// 也全撞在同一批死地址上。按 `last_attempt` 升序轮转后，候选才会真正铺开。
    pub fn metadata_candidates(&self, limit: usize, now: i64) -> Vec<(InfoHash, SocketAddr)> {
        let inner = lock(&self.inner);
        let mut candidates: Vec<&Stored> = inner
            .torrents
            .values()
            .filter(|stored| {
                stored.entry.metadata.retryable()
                    && stored.attempts < METADATA_MAX_ATTEMPTS
                    && stored.retry_after <= now
                    && !stored.peers.is_empty()
            })
            .collect();
        // 主键是「最久没试过」，同批内 peer 多的优先——peer 多说明还有人在做种。
        candidates.sort_by(|left, right| {
            left.last_attempt
                .cmp(&right.last_attempt)
                .then_with(|| right.peers.len().cmp(&left.peers.len()))
        });
        candidates
            .into_iter()
            .take(limit)
            .filter_map(|stored| {
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
        self.query_filtered(needle, sort, offset, limit, false)
    }

    /// 查询一页结果，可只保留已解析出名字的条目。
    pub fn query_filtered(
        &self,
        needle: Option<&str>,
        sort: Sort,
        offset: usize,
        limit: usize,
        named_only: bool,
    ) -> (Vec<TorrentEntry>, usize) {
        let inner = lock(&self.inner);
        let needle = needle
            .map(str::to_lowercase)
            .filter(|text| !text.is_empty());
        let mut items: Vec<TorrentEntry> = inner
            .torrents
            .values()
            .filter(|stored| !named_only || stored.entry.name.is_some())
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
    ///
    /// 最近观测到的优先：刚采到的 infohash 更可能还有人做种。
    pub fn peerless_pending(&self, limit: usize, now: i64) -> Vec<InfoHash> {
        let inner = lock(&self.inner);
        let mut items: Vec<&Stored> = inner
            .torrents
            .values()
            .filter(|stored| {
                stored.peers.len() < PEERS_MIN_FOR_METADATA
                    && stored.entry.metadata.retryable()
                    && stored.retry_after <= now
            })
            .collect();
        items.sort_by_key(|stored| std::cmp::Reverse(stored.entry.last_seen));
        items
            .into_iter()
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

    /// 载入数据库里已解析出名字的条目（进程启动时调用），返回实际载入条数。
    ///
    /// 已存在的条目保留内存里的观测数据（peer、时间），只用库里的名字补上元数据。
    pub fn seed(&self, rows: Vec<SavedTorrent>) -> usize {
        let mut inner = lock(&self.inner);
        let mut loaded = 0usize;
        for row in rows {
            if let Some(stored) = inner.torrents.get_mut(&row.infohash) {
                if stored.entry.name.is_none() {
                    stored.entry.name = Some(row.name);
                    stored.entry.total_size = row.total_size;
                    stored.entry.file_count = row.file_count;
                    stored.entry.metadata = MetadataState::Ready;
                }
                continue;
            }
            inner.torrents.insert(
                row.infohash,
                Stored {
                    entry: TorrentEntry {
                        infohash: row.infohash,
                        name: Some(row.name),
                        total_size: row.total_size,
                        file_count: row.file_count,
                        peers: row.peers,
                        first_seen: row.first_seen,
                        last_seen: row.last_seen,
                        metadata: MetadataState::Ready,
                    },
                    // 载入的条目没有 peer 地址（库里不存），也就不会重抓元数据。
                    peers: Vec::new(),
                    attempts: 0,
                    retry_after: 0,
                    persisted_at: row.last_seen,
                    last_attempt: 0,
                },
            );
            loaded += 1;
        }
        if inner.torrents.len() > self.capacity {
            Self::evict(&mut inner, self.capacity);
        }
        loaded
    }

    /// 导入已保存的条目（按 infohash 去重），返回新增到内存列表的条数。
    ///
    /// 已存在的条目只补缺失的元数据并扩展时间窗；导入的条目没有 peer 地址，
    /// 因此不会触发元数据重抓，也不会重复排队落库。
    pub fn import_saved(&self, rows: Vec<SavedTorrent>) -> usize {
        let mut inner = lock(&self.inner);
        let mut added = 0usize;
        for row in rows {
            if let Some(stored) = inner.torrents.get_mut(&row.infohash) {
                let entry = &mut stored.entry;
                if entry.name.is_none() {
                    entry.name = Some(row.name);
                    entry.total_size = row.total_size;
                    entry.file_count = row.file_count;
                    entry.metadata = MetadataState::Ready;
                } else {
                    entry.total_size = entry.total_size.or(row.total_size);
                    entry.file_count = entry.file_count.or(row.file_count);
                }
                entry.peers = entry.peers.max(row.peers);
                if row.first_seen > 0 {
                    entry.first_seen = if entry.first_seen <= 0 {
                        row.first_seen
                    } else {
                        entry.first_seen.min(row.first_seen)
                    };
                }
                entry.last_seen = entry.last_seen.max(row.last_seen);
                continue;
            }
            inner.torrents.insert(
                row.infohash,
                Stored {
                    entry: TorrentEntry {
                        infohash: row.infohash,
                        name: Some(row.name),
                        total_size: row.total_size,
                        file_count: row.file_count,
                        peers: row.peers,
                        first_seen: row.first_seen,
                        last_seen: row.last_seen,
                        metadata: MetadataState::Ready,
                    },
                    // 导入的条目没有 peer 地址（导出文件不带），不会重抓元数据。
                    peers: Vec::new(),
                    attempts: 0,
                    retry_after: 0,
                    persisted_at: row.last_seen,
                    last_attempt: 0,
                },
            );
            added += 1;
        }
        if inner.torrents.len() > self.capacity {
            Self::evict(&mut inner, self.capacity);
        }
        added
    }

    /// 从内存列表移除条目（归档成功后调用），返回实际移除条数。
    pub fn remove_saved(&self, hashes: &[InfoHash]) -> usize {
        let mut inner = lock(&self.inner);
        let mut removed = 0usize;
        for infohash in hashes {
            if inner.torrents.remove(infohash).is_some() {
                removed += 1;
            }
            inner.pending_persist.remove(infohash);
        }
        removed
    }

    /// 取一批待写库的条目。取出的条目视为「已交付」；写库失败请用
    /// [`Self::requeue_persist`] 放回队列。
    pub fn take_pending_persist(&self, limit: usize) -> Vec<SavedTorrent> {
        let mut inner = lock(&self.inner);
        if inner.pending_persist.is_empty() || limit == 0 {
            return Vec::new();
        }
        let hashes: Vec<InfoHash> = inner.pending_persist.iter().copied().take(limit).collect();
        let mut rows = Vec::with_capacity(hashes.len());
        for infohash in hashes {
            inner.pending_persist.remove(&infohash);
            let Some(stored) = inner.torrents.get(&infohash) else {
                // 已被容量淘汰：没有可写的值了。
                continue;
            };
            let Some(name) = stored.entry.name.clone() else {
                continue;
            };
            rows.push(SavedTorrent {
                infohash,
                name,
                total_size: stored.entry.total_size,
                file_count: stored.entry.file_count,
                peers: stored.entry.peers,
                first_seen: stored.entry.first_seen,
                last_seen: stored.entry.last_seen,
            });
        }
        rows
    }

    /// 把写库失败的条目放回队列。
    pub fn requeue_persist(&self, rows: &[SavedTorrent]) {
        let mut inner = lock(&self.inner);
        for row in rows {
            if inner.torrents.contains_key(&row.infohash) {
                inner.pending_persist.insert(row.infohash);
            }
        }
    }

    /// 清空全部聚合结果（含待写队列）。
    pub fn clear(&self) {
        let mut inner = lock(&self.inner);
        inner.torrents.clear();
        inner.pending_persist.clear();
        inner.observed = 0;
    }
}

/// 落库用的种子摘要：只有已解析出名字的条目才会被持久化。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedTorrent {
    /// infohash。
    pub infohash: InfoHash,
    /// 种子名。
    pub name: String,
    /// 总字节数。
    pub total_size: Option<u64>,
    /// 文件数。
    pub file_count: Option<u32>,
    /// 观测到的 peer 数。
    pub peers: u32,
    /// 首次观测时间（Unix 秒）。
    pub first_seen: i64,
    /// 最近观测时间（Unix 秒）。
    pub last_seen: i64,
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

/// 小写十六进制转 20 字节摘要；长度或字符非法时返回 `None`。
pub fn parse_hex(text: &str) -> Option<InfoHash> {
    if text.len() != 40 {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = [0u8; 20];
    for (index, slot) in out.iter_mut().enumerate() {
        let high = hex_value(bytes[index * 2])?;
        let low = hex_value(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Some(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
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

    /// 测试里的「当前时间」；显式传入让冷却逻辑可测。
    const NOW: i64 = 1_000;

    #[test]
    fn metadata_state_machine_limits_attempts_and_cools_down() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), NOW);
        assert!(store.begin_metadata(hash(1), NOW));
        // 抓取中不再重复进入。
        assert!(!store.begin_metadata(hash(1), NOW));
        store.fail_metadata(hash(1), NOW);
        for _ in 1..METADATA_MAX_ATTEMPTS {
            assert!(store.begin_metadata(hash(1), NOW));
            store.fail_metadata(hash(1), NOW);
        }
        // 一轮打完进入冷却：暂时不再尝试，也不进候选。
        assert!(!store.begin_metadata(hash(1), NOW));
        assert!(store.metadata_candidates(10, NOW).is_empty());
        // 冷却到点后重新给一轮机会（长期运行才能把名字补齐）。
        let later = NOW + METADATA_COOLDOWN_SECS;
        assert!(store.begin_metadata(hash(1), later));
        store.fail_metadata(hash(1), later);
        assert_eq!(store.metadata_candidates(10, later).len(), 1);
    }

    #[test]
    fn metadata_attempts_rotate_through_peers() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), NOW);
        store.observe(hash(1), Some(peer(2)), NOW);
        let mut seen = Vec::new();
        for _ in 0..METADATA_MAX_ATTEMPTS {
            let candidates = store.metadata_candidates(1, NOW);
            assert_eq!(candidates.len(), 1);
            seen.push(candidates[0].1);
            assert!(store.begin_metadata(hash(1), NOW));
            store.fail_metadata(hash(1), NOW);
        }
        // 每次重试换一个 peer，而不是一直撞同一具尸体。
        assert!(seen.contains(&peer(1)));
        assert!(seen.contains(&peer(2)));
    }

    #[test]
    fn metadata_summary_fills_entry_and_queues_persist() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), NOW);
        assert!(store.begin_metadata(hash(1), NOW));
        store.set_metadata(
            hash(1),
            MetadataSummary {
                name: Some("ubuntu.iso".to_owned()),
                total_size: Some(1234),
                file_count: Some(1),
            },
            NOW,
        );
        let (page, _) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(page[0].name.as_deref(), Some("ubuntu.iso"));
        assert_eq!(page[0].total_size, Some(1234));
        assert_eq!(page[0].metadata, MetadataState::Ready);
        assert_eq!(store.stats().with_metadata, 1);
        assert!(store.ready_hashes().contains(&hash(1)));

        // 解析出名字即排队落库；取走一次后队列为空。
        let batch = store.take_pending_persist(10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].infohash, hash(1));
        assert_eq!(batch[0].name, "ubuntu.iso");
        assert!(store.take_pending_persist(10).is_empty());
    }

    #[test]
    fn failed_persist_can_be_requeued() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), NOW);
        store.set_metadata(
            hash(1),
            MetadataSummary {
                name: Some("retry.iso".to_owned()),
                total_size: None,
                file_count: None,
            },
            NOW,
        );
        let batch = store.take_pending_persist(10);
        assert_eq!(batch.len(), 1);
        store.requeue_persist(&batch);
        assert_eq!(store.take_pending_persist(10).len(), 1);
    }

    #[test]
    fn observations_touch_persist_only_after_interval() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), NOW);
        store.set_metadata(
            hash(1),
            MetadataSummary {
                name: Some("steady.iso".to_owned()),
                total_size: None,
                file_count: None,
            },
            NOW,
        );
        assert_eq!(store.take_pending_persist(10).len(), 1);
        // 刚刷过库：短时间内的观测不再排队。
        store.observe(hash(1), Some(peer(2)), NOW + 10);
        assert!(store.take_pending_persist(10).is_empty());
        // 超过节流窗口才刷新 `last_seen`。
        store.observe(hash(1), Some(peer(3)), NOW + PERSIST_TOUCH_SECS);
        let batch = store.take_pending_persist(10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].last_seen, NOW + PERSIST_TOUCH_SECS);
        assert_eq!(batch[0].peers, 3);
    }

    #[test]
    fn seed_restores_saved_names() {
        let store = Store::new(64);
        let loaded = store.seed(vec![SavedTorrent {
            infohash: hash(0x5a),
            name: "arch.iso".to_owned(),
            total_size: Some(2048),
            file_count: Some(1),
            peers: 4,
            first_seen: 100,
            last_seen: 900,
        }]);
        assert_eq!(loaded, 1);
        let (page, _) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(page[0].name.as_deref(), Some("arch.iso"));
        assert_eq!(page[0].metadata, MetadataState::Ready);
        assert_eq!(page[0].peers, 4);
        assert_eq!(store.stats().with_metadata, 1);
        // 载入的条目不会重新排队落库，也不会去抓元数据。
        assert!(store.take_pending_persist(10).is_empty());
        assert!(store.metadata_candidates(10, NOW).is_empty());
    }

    #[test]
    fn seed_keeps_existing_observations() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 500);
        let loaded = store.seed(vec![SavedTorrent {
            infohash: hash(1),
            name: "known.iso".to_owned(),
            total_size: None,
            file_count: None,
            peers: 9,
            first_seen: 100,
            last_seen: 200,
        }]);
        assert_eq!(loaded, 0);
        let (page, _) = store.query(None, Sort::Recent, 0, 10);
        // 名字来自库，观测时间与 peer 数保留内存里的（更新）。
        assert_eq!(page[0].name.as_deref(), Some("known.iso"));
        assert_eq!(page[0].last_seen, 500);
        assert_eq!(page[0].peers, 1);
    }

    #[test]
    fn import_adds_new_entries_and_fills_existing() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), 500);
        let added = store.import_saved(vec![
            SavedTorrent {
                infohash: hash(1),
                name: "imported.iso".to_owned(),
                total_size: Some(42),
                file_count: Some(1),
                peers: 4,
                first_seen: 100,
                last_seen: 300,
            },
            SavedTorrent {
                infohash: hash(2),
                name: "new.iso".to_owned(),
                total_size: None,
                file_count: None,
                peers: 2,
                first_seen: 50,
                last_seen: 60,
            },
        ]);
        assert_eq!(added, 1, "已存在的条目只合并，不重复添加");
        let (page, total) = store.query(None, Sort::Recent, 0, 10);
        assert_eq!(total, 2);
        let existing = page
            .iter()
            .find(|entry| entry.infohash == hash(1))
            .expect("existing");
        assert_eq!(existing.name.as_deref(), Some("imported.iso"));
        assert_eq!(existing.total_size, Some(42));
        assert_eq!(existing.peers, 4, "peer 数取较大值");
        assert_eq!(existing.first_seen, 100, "首次观测时间取较早值");
        assert_eq!(existing.last_seen, 500, "最近观测时间取较晚值");
        assert_eq!(existing.metadata, MetadataState::Ready);
        // 导入不排队落库，也不会触发元数据重抓。
        assert!(store.take_pending_persist(10).is_empty());
        assert!(store.metadata_candidates(10, NOW).is_empty());

        // 再导入同一批：全部命中已有条目。
        assert_eq!(
            store.import_saved(vec![SavedTorrent {
                infohash: hash(2),
                name: "new.iso".to_owned(),
                total_size: None,
                file_count: None,
                peers: 2,
                first_seen: 50,
                last_seen: 60,
            }]),
            0
        );
    }

    #[test]
    fn remove_saved_drops_entries_and_pending_persist() {
        let store = Store::new(64);
        store.observe(hash(1), Some(peer(1)), NOW);
        store.set_metadata(
            hash(1),
            MetadataSummary {
                name: Some("gone.iso".to_owned()),
                total_size: None,
                file_count: None,
            },
            NOW,
        );
        assert_eq!(store.remove_saved(&[hash(1)]), 1);
        assert_eq!(store.query(None, Sort::Recent, 0, 10).1, 0);
        // 待写队列里的条目也一并丢弃，避免归档后又被刷回库。
        assert!(store.take_pending_persist(10).is_empty());
        // 重复移除不报错。
        assert_eq!(store.remove_saved(&[hash(1)]), 0);
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
            NOW,
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
    fn eviction_keeps_named_entries_over_anonymous() {
        let store = Store::new(16);
        // 一条有名字的条目，观测时间最旧。
        store.observe(hash(0), None, 1);
        store.set_metadata(
            hash(0),
            MetadataSummary {
                name: Some("keep.iso".to_owned()),
                total_size: None,
                file_count: None,
            },
            1,
        );
        // 再灌一批更新但没名字的条目，把容量顶满。
        for seed in 1..20u8 {
            store.observe(hash(seed), None, 100);
        }
        let (page, total) = store.query(None, Sort::Recent, 0, 100);
        assert_eq!(total, 16);
        // 有名字的条目即使最旧也要留下：列表不能被纯 hash 条目挤满。
        assert!(page.iter().any(|entry| entry.infohash == hash(0)));
        assert!(page.iter().any(|entry| entry.name.is_some()));
    }

    #[test]
    fn candidates_rotate_instead_of_repeating_the_same_batch() {
        let store = Store::new(64);
        for seed in 0..8u8 {
            store.observe(hash(seed), Some(peer(u16::from(seed) + 1)), NOW);
        }
        let mut seen = std::collections::HashSet::new();
        // 每次只取 1 个：应当逐个轮转，而不是反复取同一条。
        for step in 0..8i64 {
            let candidates = store.metadata_candidates(1, NOW + step);
            assert_eq!(candidates.len(), 1);
            let infohash = candidates[0].0;
            assert!(seen.insert(infohash), "候选重复: {infohash:?}");
            assert!(store.begin_metadata(infohash, NOW + step));
        }
        assert_eq!(seen.len(), 8);
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
