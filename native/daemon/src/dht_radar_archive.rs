//! DHT 雷达溢出自归档：存档超过阈值时把最久未观测的部分打包，成功写盘后再从数据库与内存删除。
//!
//! 触发点都是「有活动」的路径（落库批次、导入、配置变更）：空闲时不做周期性读取，
//! 符合 daemon 的空闲静默约束。

use std::path::{Path, PathBuf};

use fluxdown_engine::db::Db;
use fluxdown_engine::dht_radar::Radar;
use fluxdown_engine::dht_radar::archive::{ARCHIVE_DIR_NAME, write_archive};
use fluxdown_engine::dht_radar::store::InfoHash;

/// 一次自动归档的结果。
pub(crate) struct ArchivedRadar {
    /// 归档文件路径。
    pub path: PathBuf,
    /// 归档并从列表删除的条目数。
    pub rows: u64,
}

/// daemon 数据目录下的雷达归档目录。
pub(crate) fn archive_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(ARCHIVE_DIR_NAME)
}

/// 超过 `radar.config().archive_threshold` 时归档最久未观测的条目。
///
/// 顺序固定为「先写文件、后删库与内存」：写盘失败直接返回错误，数据保持原样；
/// 删库失败时归档文件已经落盘，最多多出一份重复数据，不会丢。
pub(crate) async fn archive_overflow(
    radar: &Radar,
    db: &Db,
    archive_dir: &Path,
) -> Result<Option<ArchivedRadar>, String> {
    let keep = radar.config().archive_threshold;
    if keep == 0 {
        return Ok(None);
    }
    let keep = u64::try_from(keep).unwrap_or(u64::MAX);
    let count = db
        .count_dht_radar_torrents()
        .await
        .map_err(|error| format!("count dht radar saved rows failed: {error}"))?;
    if count <= keep {
        return Ok(None);
    }
    let rows = db
        .old_dht_radar_torrents(keep)
        .await
        .map_err(|error| format!("load old dht radar rows failed: {error}"))?;
    if rows.is_empty() {
        return Ok(None);
    }
    let now = fluxdown_engine::dht_radar::now_unix();
    let path = write_archive(archive_dir, &rows, now).map_err(|error| {
        format!(
            "write dht radar archive `{}` failed: {error}",
            archive_dir.display()
        )
    })?;
    let hashes: Vec<InfoHash> = rows.iter().map(|row| row.infohash).collect();
    let removed = db
        .delete_dht_radar_torrents(&hashes)
        .await
        .map_err(|error| format!("delete archived dht radar rows failed: {error}"))?;
    radar.remove_saved(&hashes);
    if let Ok(count) = db.count_dht_radar_torrents().await {
        radar.set_saved_rows(count);
    }
    Ok(Some(ArchivedRadar {
        path,
        rows: removed,
    }))
}

#[cfg(test)]
mod tests {
    use fluxdown_engine::db::Db;
    use fluxdown_engine::dht_radar::store::{SavedTorrent, Sort, parse_hex};
    use fluxdown_engine::dht_radar::{Radar, RadarConfig};

    use super::{archive_dir, archive_overflow};

    fn saved(seed: u8, name: &str, last_seen: i64) -> SavedTorrent {
        SavedTorrent {
            infohash: [seed; 20],
            name: name.to_owned(),
            total_size: Some(10),
            file_count: Some(1),
            peers: 1,
            first_seen: 1,
            last_seen,
        }
    }

    #[tokio::test]
    async fn radar_archive_writes_file_and_drops_overflow_rows() {
        let dir = std::env::temp_dir().join(format!(
            "fluxdown_daemon_radar_archive_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("test dir");
        let db = Db::connect("sqlite::memory:").await.expect("memory db");
        let rows = vec![
            saved(1, "oldest.iso", 100),
            saved(2, "middle.iso", 200),
            saved(3, "newest.iso", 300),
        ];
        db.upsert_dht_radar_torrents(&rows)
            .await
            .expect("insert rows");
        let radar = Radar::new(RadarConfig {
            enabled: false,
            port: 0,
            capacity: 64,
            metadata: false,
            nodes: 1,
            archive_threshold: 2,
        });
        radar.seed_saved(rows);
        radar.set_saved_rows(3);

        let archived = archive_overflow(&radar, &db, &archive_dir(&dir))
            .await
            .expect("archive")
            .expect("overflow archived");
        assert_eq!(archived.rows, 1, "只归档超出阈值的部分");
        assert!(archived.path.exists());
        assert_eq!(db.count_dht_radar_torrents().await.expect("count"), 2);
        let remaining = db.load_dht_radar_torrents(10).await.expect("load");
        assert!(
            remaining
                .iter()
                .all(|row| row.infohash != parse_hex(&"01".repeat(20)).expect("hash"))
        );
        // 内存列表同步删除。
        assert_eq!(radar.list(None, Sort::Recent, 0, 10).1, 2);
        assert_eq!(radar.snapshot().saved, 2);

        // 已回落到阈值以内：再调用是空操作。
        assert!(
            archive_overflow(&radar, &db, &archive_dir(&dir))
                .await
                .expect("noop")
                .is_none()
        );

        drop(radar);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
