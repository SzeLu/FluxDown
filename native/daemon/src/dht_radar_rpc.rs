//! DHT 种子雷达 RPC（`daemon.dht.radar.*`）。作为 `service` 的子模块编译，复用其私有辅助函数。
//!
//! 雷达本身跑在引擎侧的独立线程里（UDP 采集 + 元数据抓取），这里只做参数解析、DTO 映射与
//! 配置持久化。列表查询是纯内存操作，不需要经过 actor。

use fluxdown_engine::dht_radar::archive::{encode_export, parse_import};
use fluxdown_engine::dht_radar::store::Sort;
use fluxdown_engine::dht_radar::{RadarConfig, RadarSnapshot};
use fluxdown_protocol::{
    ApplicationErrorCode, DhtRadarClearDto, DhtRadarConfigDto, DhtRadarExportDto,
    DhtRadarImportDto, DhtRadarImportParams, DhtRadarItemDto, DhtRadarListDto, DhtRadarListParams,
    DhtRadarSetConfigParams, DhtRadarStatsDto, RpcErrorData, RpcErrorObject, method,
};
use serde_json::Value;

use super::{
    DaemonService, internal_error, invalid_argument, parse_optional_params, parse_params, to_value,
};

/// 配置持久化键（与 daemon 配置表共用命名空间）。
const KEY_ENABLED: &str = "dht_radar_enabled";
const KEY_PORT: &str = "dht_radar_port";
const KEY_CAPACITY: &str = "dht_radar_capacity";
const KEY_METADATA: &str = "dht_radar_metadata";
const KEY_NODES: &str = "dht_radar_nodes";
const KEY_ARCHIVE_THRESHOLD: &str = "dht_radar_archive_threshold";

/// 单页上限，避免一次拉爆 UI。
const MAX_LIMIT: u32 = 500;
/// 单次导入的文本上限：客户端已按 16 MiB 读盘，这里再兜一层。
const MAX_IMPORT_BYTES: usize = 16 * 1024 * 1024;

fn unavailable() -> RpcErrorObject {
    RpcErrorObject::application(
        "dht radar is not available",
        RpcErrorData::new(ApplicationErrorCode::Unavailable, false),
    )
}

fn config_dto(config: RadarConfig) -> DhtRadarConfigDto {
    DhtRadarConfigDto {
        enabled: config.enabled,
        port: config.port,
        capacity: u32::try_from(config.capacity).unwrap_or(u32::MAX),
        metadata: config.metadata,
        nodes: u32::try_from(config.nodes).unwrap_or(u32::MAX),
        archive_threshold: u32::try_from(config.archive_threshold).unwrap_or(u32::MAX),
    }
}

fn stats_dto(snapshot: RadarSnapshot) -> DhtRadarStatsDto {
    DhtRadarStatsDto {
        running: snapshot.running,
        port: snapshot.port,
        packets_received: snapshot.packets_received,
        packets_sent: snapshot.packets_sent,
        queries: snapshot.queries,
        responses: snapshot.responses,
        nodes: snapshot.nodes,
        instances: snapshot.instances,
        infohashes: snapshot.infohashes,
        peers: snapshot.peers,
        metadata_ok: snapshot.metadata_ok,
        metadata_failed: snapshot.metadata_failed,
        last_packet: snapshot.last_packet,
        saved: snapshot.saved,
        torrents: snapshot.store.torrents as u64,
        with_metadata: snapshot.store.with_metadata as u64,
    }
}

impl DaemonService {
    pub(super) async fn dht_radar_call(
        &self,
        method_name: &str,
        params: Option<Value>,
    ) -> Result<Value, RpcErrorObject> {
        let radar = self.dht_radar.as_ref().ok_or_else(unavailable)?;
        match method_name {
            method::DAEMON_DHT_RADAR_LIST => {
                let params = parse_optional_params::<DhtRadarListParams>(params)?;
                let (items, total) = radar.list_filtered(
                    params.query.as_deref(),
                    Sort::parse(params.sort.as_deref()),
                    params.offset as usize,
                    params.limit.clamp(1, MAX_LIMIT) as usize,
                    params.named_only,
                );
                to_value(DhtRadarListDto {
                    total: total as u64,
                    items: items.into_iter().map(item_dto).collect(),
                })
            }
            method::DAEMON_DHT_RADAR_STATS => to_value(stats_dto(radar.snapshot())),
            method::DAEMON_DHT_RADAR_CONFIG => to_value(config_dto(radar.config())),
            method::DAEMON_DHT_RADAR_CONFIG_SET => {
                let params = parse_optional_params::<DhtRadarSetConfigParams>(params)?;
                let mut config = radar.config();
                if let Some(enabled) = params.enabled {
                    config.enabled = enabled;
                }
                if let Some(port) = params.port {
                    config.port = port;
                }
                if let Some(capacity) = params.capacity {
                    config.capacity = capacity.max(16) as usize;
                }
                if let Some(metadata) = params.metadata {
                    config.metadata = metadata;
                }
                if let Some(nodes) = params.nodes {
                    config.nodes = nodes.max(1) as usize;
                }
                if let Some(threshold) = params.archive_threshold {
                    config.archive_threshold = usize::try_from(threshold).unwrap_or(usize::MAX);
                }
                radar.update_config(config).map_err(|error| {
                    internal_error(format!("dht radar restart failed: {error}"))
                })?;
                self.persist_radar_config(config).await?;
                // 阈值可能被调低：立刻把超出的部分归档。
                self.archive_radar_overflow_logged().await;
                to_value(config_dto(radar.config()))
            }
            method::DAEMON_DHT_RADAR_CLEAR => {
                // 先清内存与待写队列，再删库：避免刷库任务把刚清掉的条目又写回来。
                radar.clear_all();
                let removed = self.db.clear_dht_radar_torrents().await.map_err(|error| {
                    internal_error(format!("clear dht radar saved rows failed: {error}"))
                })?;
                radar.set_saved_rows(0);
                to_value(DhtRadarClearDto { removed })
            }
            method::DAEMON_DHT_RADAR_EXPORT => {
                let rows = self.db.all_dht_radar_torrents().await.map_err(|error| {
                    internal_error(format!("load dht radar saved rows failed: {error}"))
                })?;
                let text = encode_export(&rows, fluxdown_engine::dht_radar::now_unix()).map_err(
                    |error| internal_error(format!("encode dht radar export failed: {error}")),
                )?;
                to_value(DhtRadarExportDto {
                    count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
                    text,
                })
            }
            method::DAEMON_DHT_RADAR_IMPORT => {
                let params = parse_params::<DhtRadarImportParams>(params)?;
                if params.text.len() > MAX_IMPORT_BYTES {
                    return Err(invalid_argument("text", "import file is too large"));
                }
                let parsed = parse_import(&params.text)
                    .map_err(|error| invalid_argument("text", &error.to_string()))?;
                // 先落库（`ON CONFLICT DO NOTHING` 去重），成功后才进内存列表。
                let imported = self
                    .db
                    .import_dht_radar_torrents(&parsed.rows)
                    .await
                    .map_err(|error| {
                        internal_error(format!("import dht radar saved rows failed: {error}"))
                    })?;
                let skipped = parsed.total.saturating_sub(imported);
                radar.import_saved(parsed.rows);
                if let Ok(count) = self.db.count_dht_radar_torrents().await {
                    radar.set_saved_rows(count);
                }
                // 导入可能把存档推过阈值：立刻归档。
                self.archive_radar_overflow_logged().await;
                to_value(DhtRadarImportDto {
                    imported,
                    skipped,
                    invalid: parsed.invalid,
                })
            }
            _ => Err(unavailable()),
        }
    }

    /// 触发一次 DHT 雷达溢出自归档；失败只记日志，不影响调用方结果。
    async fn archive_radar_overflow_logged(&self) {
        let Some(radar) = self.dht_radar.as_ref() else {
            return;
        };
        let archive_dir = crate::dht_radar_archive::archive_dir(&self.data_dir);
        match crate::dht_radar_archive::archive_overflow(radar, &self.db, &archive_dir).await {
            Ok(Some(archived)) => tracing::info!(
                path = %archived.path.display(),
                rows = archived.rows,
                "dht radar overflow archived"
            ),
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "dht radar auto archive failed"),
        }
    }

    /// 把雷达配置写回 daemon 配置表；写失败不改变已生效的内存配置，但要留下日志。
    async fn persist_radar_config(&self, config: RadarConfig) -> Result<(), RpcErrorObject> {
        let pairs = [
            (KEY_ENABLED, bool_text(config.enabled)),
            (KEY_PORT, config.port.to_string()),
            (KEY_CAPACITY, config.capacity.to_string()),
            (KEY_METADATA, bool_text(config.metadata)),
            (KEY_NODES, config.nodes.to_string()),
            (KEY_ARCHIVE_THRESHOLD, config.archive_threshold.to_string()),
        ];
        for (key, value) in pairs {
            if let Err(error) = self.db.set_config(key, &value).await {
                return Err(internal_error(format!(
                    "persist dht radar config `{key}` failed: {error}"
                )));
            }
        }
        Ok(())
    }
}

fn bool_text(value: bool) -> String {
    if value { "true" } else { "false" }.to_owned()
}

fn item_dto(entry: fluxdown_engine::dht_radar::store::TorrentEntry) -> DhtRadarItemDto {
    let magnet = entry.magnet();
    DhtRadarItemDto {
        info_hash: entry.info_hash_hex(),
        has_metadata: entry.metadata == fluxdown_engine::dht_radar::store::MetadataState::Ready,
        name: entry.name,
        total_size: entry.total_size,
        file_count: entry.file_count,
        peers: entry.peers,
        first_seen: entry.first_seen,
        last_seen: entry.last_seen,
        magnet,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fluxdown_engine::dht_radar::store::{MetadataSummary, SavedTorrent, Sort};
    use fluxdown_engine::dht_radar::{Radar, RadarConfig};
    use fluxdown_protocol::{
        ApplicationErrorCode, DhtRadarClearDto, DhtRadarConfigDto, DhtRadarExportDto,
        DhtRadarImportDto, DhtRadarListDto, DhtRadarStatsDto, method,
    };
    use serde_json::{Value, json};

    use super::super::DaemonService;

    struct Fixture {
        service: DaemonService,
        radar: Arc<Radar>,
        dir: std::path::PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.dir) {
                eprintln!("dht radar rpc test dir removal failed: {error}");
            }
        }
    }

    async fn fixture(wired: bool) -> Fixture {
        let dir = std::env::temp_dir().join(format!("fluxdown_dht_radar_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("test dir");
        let db = fluxdown_engine::db::Db::connect("sqlite::memory:")
            .await
            .expect("memory db");
        let events =
            crate::event_hub::DaemonEventHub::new(fluxdown_protocol::DaemonSnapshot::default(), 64);
        let selections = crate::selection::DaemonSelection::new(events.clone());
        let blobs = Arc::new(
            crate::blob_store::BlobStore::open(dir.join("blobs"))
                .await
                .expect("blob store"),
        );
        let radar = Radar::new(RadarConfig {
            enabled: false,
            port: 0,
            capacity: 64,
            metadata: false,
            nodes: 1,
            // 测试不触发自动归档，避免写到临时目录之外。
            archive_threshold: 0,
        });
        let service = DaemonService::new(
            crate::service_hello("dht-radar-rpc-test", Vec::new()),
            events,
            selections,
            blobs,
            crate::actor::DaemonActorHandle::disconnected(),
            db,
            dir.clone(),
            #[cfg(feature = "plugins")]
            None,
        );
        let service = if wired {
            service.with_dht_radar(Arc::clone(&radar))
        } else {
            service
        };
        Fixture {
            service,
            radar,
            dir,
        }
    }

    async fn call(
        fx: &Fixture,
        name: &str,
        params: Option<Value>,
    ) -> Result<Value, fluxdown_protocol::RpcErrorObject> {
        fx.service
            .call_inner("dht-radar-rpc-test", false, name, params)
            .await
    }

    fn code(error: &fluxdown_protocol::RpcErrorObject) -> ApplicationErrorCode {
        error
            .data
            .as_ref()
            .map(|data| data.code)
            .expect("error code")
    }

    #[tokio::test]
    async fn unwired_service_answers_unavailable() {
        let fx = fixture(false).await;
        let error = call(&fx, method::DAEMON_DHT_RADAR_LIST, None)
            .await
            .expect_err("must be unavailable");
        assert_eq!(code(&error), ApplicationErrorCode::Unavailable);
        let error = call(&fx, method::DAEMON_DHT_RADAR_CONFIG, None)
            .await
            .expect_err("must be unavailable");
        assert_eq!(code(&error), ApplicationErrorCode::Unavailable);
        let error = call(&fx, method::DAEMON_DHT_RADAR_CLEAR, None)
            .await
            .expect_err("must be unavailable");
        assert_eq!(code(&error), ApplicationErrorCode::Unavailable);
    }

    #[tokio::test]
    async fn config_defaults_and_patch_are_persisted() {
        let fx = fixture(true).await;
        let config: DhtRadarConfigDto = serde_json::from_value(
            call(&fx, method::DAEMON_DHT_RADAR_CONFIG, None)
                .await
                .expect("config"),
        )
        .expect("config dto");
        assert!(!config.enabled);
        assert!(!config.metadata);
        assert_eq!(config.capacity, 64);
        assert_eq!(config.nodes, 1);

        // 只改端口与容量：不启动采集，避免测试里发真实 UDP。
        let patched: DhtRadarConfigDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_CONFIG_SET,
                Some(json!({
                    "port": 0,
                    "capacity": 128,
                    "nodes": 2,
                    "archiveThreshold": 500,
                })),
            )
            .await
            .expect("config set"),
        )
        .expect("config dto");
        assert_eq!(patched.capacity, 128);
        assert_eq!(patched.nodes, 2);
        assert_eq!(patched.archive_threshold, 500);
        assert!(!patched.enabled);
        assert!(!fx.radar.is_running());

        // 配置写回了 daemon 配置表。
        assert_eq!(
            fx.service
                .db
                .get_config("dht_radar_capacity")
                .await
                .expect("read config")
                .as_deref(),
            Some("128")
        );
        assert_eq!(
            fx.service
                .db
                .get_config("dht_radar_nodes")
                .await
                .expect("read config")
                .as_deref(),
            Some("2")
        );
        assert_eq!(
            fx.service
                .db
                .get_config("dht_radar_archive_threshold")
                .await
                .expect("read config")
                .as_deref(),
            Some("500")
        );
    }

    #[tokio::test]
    async fn stats_report_running_state_and_store_totals() {
        let fx = fixture(true).await;
        fx.radar
            .store()
            .observe([7u8; 20], Some("1.2.3.4:6881".parse().expect("addr")), 100);
        let stats: DhtRadarStatsDto = serde_json::from_value(
            call(&fx, method::DAEMON_DHT_RADAR_STATS, None)
                .await
                .expect("stats"),
        )
        .expect("stats dto");
        assert!(!stats.running);
        assert_eq!(stats.torrents, 1);
        assert_eq!(stats.with_metadata, 0);
        assert_eq!(stats.instances, 1);
        assert_eq!(stats.saved, 0);
    }

    #[tokio::test]
    async fn list_maps_entries_to_dto_with_magnet() {
        let fx = fixture(true).await;
        let store = fx.radar.store();
        store.observe(
            [0xabu8; 20],
            Some("1.2.3.4:6881".parse().expect("addr")),
            100,
        );
        store.begin_metadata([0xabu8; 20], 100);
        store.set_metadata(
            [0xabu8; 20],
            MetadataSummary {
                name: Some("debian.iso".to_owned()),
                total_size: Some(1024),
                file_count: Some(1),
            },
            100,
        );
        store.observe([0xcdu8; 20], None, 200);

        let page: DhtRadarListDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_LIST,
                Some(json!({ "sort": "recent", "limit": 10 })),
            )
            .await
            .expect("list"),
        )
        .expect("list dto");
        assert_eq!(page.total, 2);
        assert_eq!(page.items.len(), 2);
        // 最近观测的排在前面。
        assert_eq!(page.items[0].info_hash, "cd".repeat(20));
        assert!(!page.items[0].has_metadata);
        let debian = &page.items[1];
        assert_eq!(debian.name.as_deref(), Some("debian.iso"));
        assert_eq!(debian.total_size, Some(1024));
        assert!(debian.has_metadata);
        assert_eq!(
            debian.magnet,
            format!("magnet:?xt=urn:btih:{}", "ab".repeat(20))
        );

        // 关键字只匹配名称与 infohash 前缀。
        let filtered: DhtRadarListDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_LIST,
                Some(json!({ "query": "deb" })),
            )
            .await
            .expect("list"),
        )
        .expect("list dto");
        assert_eq!(filtered.total, 1);
        assert_eq!(filtered.items[0].name.as_deref(), Some("debian.iso"));

        // 只要已解析出名字的。
        let named: DhtRadarListDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_LIST,
                Some(json!({ "namedOnly": true })),
            )
            .await
            .expect("list"),
        )
        .expect("list dto");
        assert_eq!(named.total, 1);
        assert_eq!(named.items[0].name.as_deref(), Some("debian.iso"));
    }

    #[tokio::test]
    async fn clear_drops_saved_rows_and_memory() {
        let fx = fixture(true).await;
        let store = fx.radar.store();
        store.observe(
            [0xabu8; 20],
            Some("1.2.3.4:6881".parse().expect("addr")),
            100,
        );
        store.set_metadata(
            [0xabu8; 20],
            MetadataSummary {
                name: Some("saved.iso".to_owned()),
                total_size: Some(10),
                file_count: Some(1),
            },
            100,
        );
        // 模拟落库任务：取队列并写库。
        let rows = fx.radar.take_persist_batch(10);
        fx.service
            .db
            .upsert_dht_radar_torrents(&rows)
            .await
            .expect("insert");
        fx.radar.set_saved_rows(1);
        assert_eq!(
            fx.service
                .db
                .count_dht_radar_torrents()
                .await
                .expect("count"),
            1
        );

        let cleared: DhtRadarClearDto = serde_json::from_value(
            call(&fx, method::DAEMON_DHT_RADAR_CLEAR, None)
                .await
                .expect("clear"),
        )
        .expect("clear dto");
        assert_eq!(cleared.removed, 1);
        assert_eq!(
            fx.service
                .db
                .count_dht_radar_torrents()
                .await
                .expect("count"),
            0
        );

        // 内存与统计一起归零。
        let page: DhtRadarListDto = serde_json::from_value(
            call(&fx, method::DAEMON_DHT_RADAR_LIST, None)
                .await
                .expect("list"),
        )
        .expect("list dto");
        assert_eq!(page.total, 0);
        let stats: DhtRadarStatsDto = serde_json::from_value(
            call(&fx, method::DAEMON_DHT_RADAR_STATS, None)
                .await
                .expect("stats"),
        )
        .expect("stats dto");
        assert_eq!(stats.saved, 0);
        assert_eq!(stats.torrents, 0);
    }

    #[tokio::test]
    async fn saved_names_survive_radar_restart() {
        let fx = fixture(true).await;
        // 第一个雷达：解析出名字并落库（模拟刷库任务）。
        let store = fx.radar.store();
        store.observe(
            [0x11u8; 20],
            Some("1.2.3.4:6881".parse().expect("addr")),
            100,
        );
        store.set_metadata(
            [0x11u8; 20],
            MetadataSummary {
                name: Some("persisted.iso".to_owned()),
                total_size: Some(4096),
                file_count: Some(1),
            },
            100,
        );
        let rows = fx.radar.take_persist_batch(10);
        fx.service
            .db
            .upsert_dht_radar_torrents(&rows)
            .await
            .expect("insert");

        // 模拟进程重启：新雷达从库里回填，列表立刻有名字。
        let restarted = Radar::new(RadarConfig {
            enabled: false,
            port: 0,
            capacity: 64,
            metadata: false,
            nodes: 1,
            // 测试不触发自动归档，避免写到临时目录之外。
            archive_threshold: 0,
        });
        let saved = fx
            .service
            .db
            .load_dht_radar_torrents(64)
            .await
            .expect("load");
        assert_eq!(restarted.seed_saved(saved), 1);
        let (items, total) = restarted.list(None, Sort::Recent, 0, 10);
        assert_eq!(total, 1);
        assert_eq!(items[0].name.as_deref(), Some("persisted.iso"));
        assert_eq!(items[0].total_size, Some(4096));
        // 载入的条目不会重复落库。
        assert!(restarted.take_persist_batch(10).is_empty());
    }

    fn saved_row(seed: u8, name: &str, last_seen: i64) -> SavedTorrent {
        SavedTorrent {
            infohash: [seed; 20],
            name: name.to_owned(),
            total_size: Some(2048),
            file_count: Some(1),
            peers: 3,
            first_seen: 10,
            last_seen,
        }
    }

    #[tokio::test]
    async fn export_returns_importable_document() {
        let fx = fixture(true).await;
        fx.service
            .db
            .upsert_dht_radar_torrents(&[
                saved_row(0xab, "debian.iso", 100),
                saved_row(0xcd, "ubuntu.iso", 200),
            ])
            .await
            .expect("insert");

        let exported: DhtRadarExportDto = serde_json::from_value(
            call(&fx, method::DAEMON_DHT_RADAR_EXPORT, None)
                .await
                .expect("export"),
        )
        .expect("export dto");
        assert_eq!(exported.count, 2);
        let parsed =
            fluxdown_engine::dht_radar::archive::parse_import(&exported.text).expect("parse");
        assert_eq!(parsed.invalid, 0);
        assert_eq!(parsed.rows.len(), 2);
        assert_eq!(parsed.rows[0].name, "ubuntu.iso");
    }

    #[tokio::test]
    async fn import_dedupes_against_saved_rows_and_memory() {
        let fx = fixture(true).await;
        fx.service
            .db
            .upsert_dht_radar_torrents(&[saved_row(0xab, "existing.iso", 100)])
            .await
            .expect("insert");
        // 模拟 daemon 启动时从库里回填内存。
        let saved = fx
            .service
            .db
            .load_dht_radar_torrents(64)
            .await
            .expect("load");
        fx.radar.seed_saved(saved);

        // 文件里同时有「库里已存在」与「新条目」，前者改名也仍然是同一条。
        let text = format!(
            r#"{{"version":1,"items":[
                {{"infoHash":"{existing}","name":"renamed.iso","lastSeen":300}},
                {{"infoHash":"{new}","name":"new.iso","totalSize":99,"lastSeen":400}}
            ]}}"#,
            existing = "ab".repeat(20),
            new = "ef".repeat(20)
        );
        let imported: DhtRadarImportDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_IMPORT,
                Some(json!({ "text": text })),
            )
            .await
            .expect("import"),
        )
        .expect("import dto");
        assert_eq!(imported.imported, 1);
        assert_eq!(imported.skipped, 1);
        assert_eq!(imported.invalid, 0);

        assert_eq!(
            fx.service
                .db
                .count_dht_radar_torrents()
                .await
                .expect("count"),
            2
        );
        // 新条目立即可见，已有条目保留原名。
        let page: DhtRadarListDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_LIST,
                Some(json!({ "limit": 10 })),
            )
            .await
            .expect("list"),
        )
        .expect("list dto");
        assert_eq!(page.total, 2);
        let names: Vec<Option<&str>> = page.items.iter().map(|item| item.name.as_deref()).collect();
        assert!(names.contains(&Some("new.iso")), "{names:?}");
        assert!(names.contains(&Some("existing.iso")), "{names:?}");

        // 再导入同一份文件：全部是重复。
        let again: DhtRadarImportDto = serde_json::from_value(
            call(
                &fx,
                method::DAEMON_DHT_RADAR_IMPORT,
                Some(json!({ "text": text })),
            )
            .await
            .expect("import again"),
        )
        .expect("import dto");
        assert_eq!(again.imported, 0);
        assert_eq!(again.skipped, 2);
    }

    #[tokio::test]
    async fn import_rejects_malformed_payload() {
        let fx = fixture(true).await;
        let error = call(
            &fx,
            method::DAEMON_DHT_RADAR_IMPORT,
            Some(json!({ "text": "not json" })),
        )
        .await
        .expect_err("invalid import");
        assert_eq!(code(&error), ApplicationErrorCode::InvalidArgument);
        let error = call(
            &fx,
            method::DAEMON_DHT_RADAR_IMPORT,
            Some(json!({ "text": "{\"version\":99,\"items\":[]}" })),
        )
        .await
        .expect_err("future version");
        assert_eq!(code(&error), ApplicationErrorCode::InvalidArgument);
    }
}
