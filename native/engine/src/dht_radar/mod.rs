//! DHT 种子雷达：被动采集 Mainline DHT 上的 infohash，聚合成可浏览的种子列表。
//!
//! 结构分三层：
//! - [`crawler`]：一个真实 DHT 节点（UDP），应答查询并从别人的 `get_peers` /
//!   `announce_peer` 里收集 infohash 与 peer 地址；
//! - [`metadata`]：BEP 9 拉取 `info` 字典，补上名称与体积；
//! - [`store`]：按 infohash 聚合去重，供 RPC 查询。
//!
//! 采集线程是普通 `std::thread` + 阻塞 socket：DHT 是超时驱动的小报文，用不上异步运行时，
//! 也避免与下载引擎的 tokio 调度抢资源。

pub mod bencode;
pub mod crawler;
pub mod krpc;
pub mod metadata;
pub mod store;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::dht_radar::store::{InfoHash, Sort, Store, StoreStats, TorrentEntry};

/// 默认监听端口（与 BT 常用端口一致；被占用时退回系统分配）。
pub const DEFAULT_PORT: u16 = 6881;
/// 默认保留的种子条数。
pub const DEFAULT_CAPACITY: usize = 20_000;
/// 默认虚拟节点数。单个随机 ID 的节点极少被别的节点查询，多开几个才收得到
/// `get_peers` / `announce_peer`（每个节点独立 UDP 端口与节点 ID）。
pub const DEFAULT_NODES: usize = 8;
/// 虚拟节点数上限：每个节点一个 UDP 端口与 2048 条节点表。
pub const MAX_NODES: usize = 32;
/// 元数据抓取并发线程数。抓取是长连接 IO，线程多一点才能跟上采集速度。
const METADATA_WORKERS: usize = 16;
/// 每个 worker 一轮取的候选条数。
const METADATA_BATCH: usize = 8;

/// 雷达配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RadarConfig {
    /// 是否开启采集。
    pub enabled: bool,
    /// UDP 监听端口；0 表示由系统分配。
    pub port: u16,
    /// 保留的种子条数上限。
    pub capacity: usize,
    /// 是否抓取元数据（关闭后只有 infohash）。
    pub metadata: bool,
    /// 虚拟节点数。
    pub nodes: usize,
}

impl Default for RadarConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: DEFAULT_PORT,
            capacity: DEFAULT_CAPACITY,
            metadata: true,
            nodes: DEFAULT_NODES,
        }
    }
}

/// 采集统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RadarSnapshot {
    /// 是否在运行。
    pub running: bool,
    /// 实际监听端口。
    pub port: u16,
    /// 收到的报文数。
    pub packets_received: u64,
    /// 发出的报文数。
    pub packets_sent: u64,
    /// 收到的查询数。
    pub queries: u64,
    /// 收到的应答数。
    pub responses: u64,
    /// 节点表大小（全部虚拟节点之和）。
    pub nodes: u64,
    /// 正在运行的虚拟节点数。
    pub instances: u32,
    /// 观测到的 infohash 总数（含已淘汰）。
    pub infohashes: u64,
    /// 记录到的 peer 次数。
    pub peers: u64,
    /// 元数据抓取成功数。
    pub metadata_ok: u64,
    /// 元数据抓取失败数。
    pub metadata_failed: u64,
    /// 最近一次收到报文的 Unix 时间（秒），0 表示还没收到过。
    pub last_packet: i64,
    /// 数据库里已保存的条目数（已解析出名字的历史种子）。
    pub saved: u64,
    /// 聚合仓库统计。
    pub store: StoreStats,
}

/// 原子计数器集合。
#[derive(Debug, Default)]
pub struct RadarStats {
    pub packets: AtomicU64,
    pub packets_sent: AtomicU64,
    pub queries: AtomicU64,
    pub responses: AtomicU64,
    pub infohashes: AtomicU64,
    pub peers: AtomicU64,
    pub metadata_ok: AtomicU64,
    pub metadata_failed: AtomicU64,
    pub nodes_known: AtomicU64,
    pub bound_port: AtomicU32,
    pub last_packet: AtomicI64,
}

/// 雷达句柄。克隆代价低（内部共享），由 daemon 持有单例。
pub struct Radar {
    store: Arc<Store>,
    stats: Arc<RadarStats>,
    config: Mutex<RadarConfig>,
    running: AtomicBool,
    stop: Mutex<Option<Arc<AtomicBool>>>,
    /// 抓元数据时用的本地 peer id（只用于握手，不参与下载）。
    peer_id: [u8; 20],
    /// 数据库里已保存的条目数（由 daemon 刷库后回填）。
    saved_rows: AtomicU64,
}

impl Radar {
    /// 按配置创建（不启动）。
    pub fn new(config: RadarConfig) -> Arc<Self> {
        Arc::new(Self {
            store: Arc::new(Store::new(config.capacity)),
            stats: Arc::new(RadarStats::default()),
            config: Mutex::new(config),
            running: AtomicBool::new(false),
            stop: Mutex::new(None),
            peer_id: krpc::random_node_id(),
            saved_rows: AtomicU64::new(0),
        })
    }

    /// 当前配置。
    pub fn config(&self) -> RadarConfig {
        *store::lock(&self.config)
    }

    /// 是否在运行。
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// 启动采集线程；已在运行时是空操作。
    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let config = self.config();
        let stop = Arc::new(AtomicBool::new(false));
        let instances = config.nodes.clamp(1, MAX_NODES);
        let mut bound = Vec::with_capacity(instances);
        for index in 0..instances {
            // 首个节点用配置端口（占用则退回系统分配），其余顺延；顺延失败也退回系统分配。
            let preferred = if config.port == 0 {
                0
            } else {
                config
                    .port
                    .saturating_add(u16::try_from(index).unwrap_or(u16::MAX))
            };
            let crawler =
                crawler::Crawler::bind(preferred, Arc::clone(&self.store), Arc::clone(&self.stats))
                    .or_else(|error| {
                        tracing::warn!(
                            %error,
                            port = preferred,
                            "dht-radar bind failed, falling back to ephemeral port"
                        );
                        crawler::Crawler::bind(0, Arc::clone(&self.store), Arc::clone(&self.stats))
                    });
            match crawler {
                Ok(crawler) => bound.push(crawler),
                Err(error) if bound.is_empty() => {
                    self.running.store(false, Ordering::SeqCst);
                    return Err(error.to_string());
                }
                Err(error) => tracing::warn!(%error, "dht-radar virtual node skipped"),
            }
        }
        let port = bound
            .first()
            .and_then(crawler::Crawler::local_port)
            .unwrap_or(0);
        *store::lock(&self.stop) = Some(Arc::clone(&stop));
        for (index, crawler) in bound.into_iter().enumerate() {
            let crawler_stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name(format!("dht-radar-{index}"))
                .spawn(move || crawler.run(crawler_stop))
                .map_err(|error| {
                    self.running.store(false, Ordering::SeqCst);
                    error.to_string()
                })?;
        }
        if config.metadata {
            for index in 0..METADATA_WORKERS {
                let store = Arc::clone(&self.store);
                let stats = Arc::clone(&self.stats);
                let stop = Arc::clone(&stop);
                let peer_id = self.peer_id;
                std::thread::Builder::new()
                    .name(format!("dht-radar-meta-{index}"))
                    .spawn(move || metadata_worker(store, stats, stop, peer_id))
                    .map_err(|error| error.to_string())?;
            }
        }
        tracing::info!(
            port,
            instances,
            capacity = config.capacity,
            "dht-radar started"
        );
        Ok(())
    }

    /// 停止采集线程。
    pub fn stop(&self) {
        if let Some(stop) = store::lock(&self.stop).take() {
            stop.store(true, Ordering::SeqCst);
        }
        self.running.store(false, Ordering::SeqCst);
    }

    /// 开 / 关采集。
    pub fn set_enabled(self: &Arc<Self>, enabled: bool) -> Result<(), String> {
        store::lock(&self.config).enabled = enabled;
        if enabled {
            self.start()
        } else {
            self.stop();
            Ok(())
        }
    }

    /// 更新容量与元数据开关（端口变化需要重启才能生效）。
    pub fn update_config(self: &Arc<Self>, config: RadarConfig) -> Result<(), String> {
        let restart = {
            let mut current = store::lock(&self.config);
            let restart = current.port != config.port && self.is_running();
            *current = config;
            restart
        };
        if config.enabled && !self.is_running() {
            self.start()?;
        }
        if restart {
            self.stop();
            self.start()?;
        }
        Ok(())
    }

    /// 查询一页结果。
    pub fn list(
        &self,
        needle: Option<&str>,
        sort: Sort,
        offset: usize,
        limit: usize,
    ) -> (Vec<TorrentEntry>, usize) {
        self.store.query(needle, sort, offset, limit)
    }

    /// 查询一页结果，可只保留已解析出名字的条目。
    pub fn list_filtered(
        &self,
        needle: Option<&str>,
        sort: Sort,
        offset: usize,
        limit: usize,
        named_only: bool,
    ) -> (Vec<TorrentEntry>, usize) {
        self.store
            .query_filtered(needle, sort, offset, limit, named_only)
    }

    /// 当前统计。
    pub fn snapshot(&self) -> RadarSnapshot {
        RadarSnapshot {
            running: self.is_running(),
            port: u16::try_from(self.stats.bound_port.load(Ordering::Relaxed)).unwrap_or(0),
            packets_received: self.stats.packets.load(Ordering::Relaxed),
            packets_sent: self.stats.packets_sent.load(Ordering::Relaxed),
            queries: self.stats.queries.load(Ordering::Relaxed),
            responses: self.stats.responses.load(Ordering::Relaxed),
            nodes: self.stats.nodes_known.load(Ordering::Relaxed),
            instances: u32::try_from(self.config().nodes.clamp(1, MAX_NODES)).unwrap_or(1),
            infohashes: self.stats.infohashes.load(Ordering::Relaxed),
            peers: self.stats.peers.load(Ordering::Relaxed),
            metadata_ok: self.stats.metadata_ok.load(Ordering::Relaxed),
            metadata_failed: self.stats.metadata_failed.load(Ordering::Relaxed),
            last_packet: self.stats.last_packet.load(Ordering::Relaxed),
            saved: self.saved_rows.load(Ordering::Relaxed),
            store: self.store.stats(),
        }
    }

    /// 载入数据库里已保存的历史条目（daemon 启动时调用）。
    pub fn seed_saved(&self, rows: Vec<store::SavedTorrent>) -> usize {
        let loaded = self.store.seed(rows);
        self.saved_rows
            .store(u64::try_from(loaded).unwrap_or(u64::MAX), Ordering::Relaxed);
        loaded
    }

    /// 取一批待写库的条目（已解析出名字的）。
    pub fn take_persist_batch(&self, limit: usize) -> Vec<store::SavedTorrent> {
        self.store.take_pending_persist(limit)
    }

    /// 写库失败时把条目放回队列，下一轮再试。
    pub fn requeue_persist(&self, rows: &[store::SavedTorrent]) {
        self.store.requeue_persist(rows);
    }

    /// 回填「数据库里已保存的条目数」。
    pub fn set_saved_rows(&self, rows: u64) {
        self.saved_rows.store(rows, Ordering::Relaxed);
    }

    /// 清空聚合结果与待写队列（`daemon.dht.radar.clear`）。
    pub fn clear_all(&self) {
        self.store.clear();
    }

    /// 聚合仓库（测试与诊断用）。
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }
}

/// 元数据抓取线程：不断从仓库取待抓条目，逐个短连接抓取。
fn metadata_worker(
    store: Arc<Store>,
    stats: Arc<RadarStats>,
    stop: Arc<AtomicBool>,
    peer_id: [u8; 20],
) {
    while !stop.load(Ordering::SeqCst) {
        let candidates = store.metadata_candidates(METADATA_BATCH, now_unix());
        if candidates.is_empty() {
            std::thread::sleep(Duration::from_millis(500));
            continue;
        }
        for (infohash, peer) in candidates {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            if !store.begin_metadata(infohash, now_unix()) {
                continue;
            }
            match metadata::fetch(&infohash, peer, &peer_id) {
                Ok(summary) => {
                    store.set_metadata(infohash, summary, now_unix());
                    stats.metadata_ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    store.fail_metadata(infohash, now_unix());
                    stats.metadata_failed.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%peer, %error, "dht-radar metadata fetch failed");
                }
            }
        }
    }
}

/// 当前 Unix 时间（秒）。
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// infohash 十六进制（RPC 与磁力链接共用）。
pub fn hex(infohash: &InfoHash) -> String {
    store::hex(infohash)
}
