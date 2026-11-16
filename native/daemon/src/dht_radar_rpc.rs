//! DHT 种子雷达 RPC（`daemon.dht.radar.*`）。作为 `service` 的子模块编译，复用其私有辅助函数。
//!
//! 雷达本身跑在引擎侧的独立线程里（UDP 采集 + 元数据抓取），这里只做参数解析、DTO 映射与
//! 配置持久化。列表查询是纯内存操作，不需要经过 actor。

use fluxdown_engine::dht_radar::store::Sort;
use fluxdown_engine::dht_radar::{RadarConfig, RadarSnapshot};
use fluxdown_protocol::{
    ApplicationErrorCode, DhtRadarConfigDto, DhtRadarItemDto, DhtRadarListDto, DhtRadarListParams,
    DhtRadarSetConfigParams, DhtRadarStatsDto, RpcErrorData, RpcErrorObject, method,
};
use serde_json::Value;

use super::{DaemonService, internal_error, parse_optional_params, to_value};

/// 配置持久化键（与 daemon 配置表共用命名空间）。
const KEY_ENABLED: &str = "dht_radar_enabled";
const KEY_PORT: &str = "dht_radar_port";
const KEY_CAPACITY: &str = "dht_radar_capacity";
const KEY_METADATA: &str = "dht_radar_metadata";
const KEY_NODES: &str = "dht_radar_nodes";

/// 单页上限，避免一次拉爆 UI。
const MAX_LIMIT: u32 = 500;

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
                let (items, total) = radar.list(
                    params.query.as_deref(),
                    Sort::parse(params.sort.as_deref()),
                    params.offset as usize,
                    params.limit.clamp(1, MAX_LIMIT) as usize,
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
                radar.update_config(config).map_err(|error| {
                    internal_error(format!("dht radar restart failed: {error}"))
                })?;
                self.persist_radar_config(config).await?;
                to_value(config_dto(radar.config()))
            }
            _ => Err(unavailable()),
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

    use fluxdown_engine::dht_radar::store::MetadataSummary;
    use fluxdown_engine::dht_radar::{Radar, RadarConfig};
    use fluxdown_protocol::{
        ApplicationErrorCode, DhtRadarConfigDto, DhtRadarListDto, DhtRadarStatsDto, method,
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
                Some(json!({ "port": 0, "capacity": 128, "nodes": 2 })),
            )
            .await
            .expect("config set"),
        )
        .expect("config dto");
        assert_eq!(patched.capacity, 128);
        assert_eq!(patched.nodes, 2);
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
        store.begin_metadata([0xabu8; 20]);
        store.set_metadata(
            [0xabu8; 20],
            MetadataSummary {
                name: Some("debian.iso".to_owned()),
                total_size: Some(1024),
                file_count: Some(1),
            },
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
    }
}
