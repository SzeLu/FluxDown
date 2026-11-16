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
