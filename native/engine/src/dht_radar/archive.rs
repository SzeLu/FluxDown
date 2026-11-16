//! 雷达发现内容的导入导出与自动归档。
//!
//! 导出文件是可**再次直接导入**的 JSON 文档（`version` + `items`）。自动归档把超出保留阈值的
//! 最旧条目写成同一格式的 gzip 压缩文件，放在数据目录的 `dht_radar_archive/` 下。归档文件
//! 写盘成功后才从数据库与内存列表删除条目：写盘失败时数据原样保留，不会丢。

use std::collections::HashMap;
use std::collections::hash_map::Entry as HashEntry;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::store::{InfoHash, SavedTorrent, parse_hex};

/// 导出 / 导入文件的格式版本。
pub const EXPORT_VERSION: u32 = 1;
/// 归档目录名（位于 daemon 数据目录下）。
pub const ARCHIVE_DIR_NAME: &str = "dht_radar_archive";
/// 单次导入允许的最大条目数：防止构造的超大文件把内存打满。
pub const MAX_IMPORT_ITEMS: usize = 200_000;
/// 导入时种子名的最大字符数（按字符截断，不会切坏 UTF-8）。
pub const MAX_NAME_CHARS: usize = 512;

/// 导出文件文档。
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportDocument {
    /// 格式版本。
    pub version: u32,
    /// 导出时间（Unix 秒）。
    pub exported_at: i64,
    /// 条目。
    pub items: Vec<ExportItem>,
}

/// 导出文件里的一条种子。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportItem {
    /// 40 位十六进制 infohash（大小写均可）。
    pub info_hash: String,
    /// 种子名。
    pub name: String,
    /// 总字节数。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_size: Option<u64>,
    /// 文件数。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_count: Option<u32>,
    /// 观测到的 peer 数。
    #[serde(default)]
    pub peers: u32,
    /// 首次观测时间（Unix 秒）。
    #[serde(default)]
    pub first_seen: i64,
    /// 最近观测时间（Unix 秒）。
    #[serde(default)]
    pub last_seen: i64,
}

/// 导入解析结果。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ParsedImport {
    /// 按 infohash 去重后的条目。
    pub rows: Vec<SavedTorrent>,
    /// 文件中解析成功的条目数（去重前）。
    pub total: u64,
    /// infohash / 名字非法而被跳过的条目数。
    pub invalid: u64,
}

/// 导入文件无法解析时的错误。
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// 不是合法 JSON，或顶层结构不对。
    #[error("invalid import file: {0}")]
    Json(#[from] serde_json::Error),
    /// 文件版本高于本程序支持的版本。
    #[error("unsupported import version {found}; this build supports up to {supported}")]
    UnsupportedVersion {
        /// 文件里的版本。
        found: u32,
        /// 本程序支持的最高版本。
        supported: u32,
    },
    /// 条目数超过 [`MAX_IMPORT_ITEMS`]。
    #[error("import file has too many items ({0})")]
    TooManyItems(usize),
}

/// 把已保存的条目编码成导出 JSON（带缩进，便于人工查看）。
pub fn encode_export(rows: &[SavedTorrent], now: i64) -> Result<String, serde_json::Error> {
    let document = ExportDocument {
        version: EXPORT_VERSION,
        exported_at: now,
        items: rows.iter().map(export_item).collect(),
    };
    serde_json::to_string_pretty(&document)
}

/// 解析导入文件：逐条宽松校验，跳过非法条目，并按 infohash 去重。
///
/// 也接受顶层直接是条目数组的旧格式。去重时合并同一条目的信息：
/// 缺失的体积 / 文件数取有值的一侧，时间窗取并集，peer 数取较大值。
pub fn parse_import(text: &str) -> Result<ParsedImport, ImportError> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawDocument {
        Document {
            #[serde(default = "default_version")]
            version: u32,
            #[serde(default)]
            items: Vec<serde_json::Value>,
        },
        Items(Vec<serde_json::Value>),
    }

    fn default_version() -> u32 {
        EXPORT_VERSION
    }

    let (version, items) = match serde_json::from_str::<RawDocument>(text)? {
        RawDocument::Document { version, items } => (version, items),
        RawDocument::Items(items) => (EXPORT_VERSION, items),
    };
    if version > EXPORT_VERSION {
        return Err(ImportError::UnsupportedVersion {
            found: version,
            supported: EXPORT_VERSION,
        });
    }
    if items.len() > MAX_IMPORT_ITEMS {
        return Err(ImportError::TooManyItems(items.len()));
    }

    let mut parsed = ParsedImport::default();
    let mut merged: HashMap<InfoHash, SavedTorrent> = HashMap::new();
    for value in items {
        let Ok(item) = serde_json::from_value::<ExportItem>(value) else {
            parsed.invalid = parsed.invalid.saturating_add(1);
            continue;
        };
        let Some(row) = sanitize(item) else {
            parsed.invalid = parsed.invalid.saturating_add(1);
            continue;
        };
        parsed.total = parsed.total.saturating_add(1);
        match merged.entry(row.infohash) {
            HashEntry::Occupied(mut existing) => merge_saved(existing.get_mut(), row),
            HashEntry::Vacant(vacant) => {
                vacant.insert(row);
            }
        }
    }
    parsed.rows = merged.into_values().collect();
    // 固定顺序：最近观测在前，便于调用方与测试稳定比较。
    parsed.rows.sort_by(|left, right| {
        right
            .last_seen
            .cmp(&left.last_seen)
            .then_with(|| left.infohash.cmp(&right.infohash))
    });
    Ok(parsed)
}

/// 把归档内容写成 gzip 压缩的 JSON 文档，返回文件路径。
///
/// 同一秒内多次归档会追加序号（`-1`、`-2`…），不会覆盖已有文件。
pub fn write_archive(dir: &Path, rows: &[SavedTorrent], now: i64) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let json = encode_export(rows, now).map_err(std::io::Error::other)?;
    let mut sequence = 0u32;
    loop {
        let file_name = if sequence == 0 {
            format!("dht-radar-{now}.json.gz")
        } else {
            format!("dht-radar-{now}-{sequence}.json.gz")
        };
        let path = dir.join(file_name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => {
                let mut encoder =
                    flate2::write::GzEncoder::new(file, flate2::Compression::default());
                encoder.write_all(json.as_bytes())?;
                encoder.finish()?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && sequence < 999 => {
                sequence = sequence.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

fn export_item(row: &SavedTorrent) -> ExportItem {
    ExportItem {
        info_hash: super::store::hex(&row.infohash),
        name: row.name.clone(),
        total_size: row.total_size,
        file_count: row.file_count,
        peers: row.peers,
        first_seen: row.first_seen,
        last_seen: row.last_seen,
    }
}

/// 校验单条导入项；infohash 非法或名字为空时返回 `None`。
fn sanitize(item: ExportItem) -> Option<SavedTorrent> {
    let infohash = parse_hex(&item.info_hash)?;
    let name = item.name.trim();
    if name.is_empty() {
        return None;
    }
    let name: String = if name.chars().count() > MAX_NAME_CHARS {
        name.chars().take(MAX_NAME_CHARS).collect()
    } else {
        name.to_owned()
    };
    let first_seen = item.first_seen.max(0);
    let last_seen = item.last_seen.max(first_seen);
    Some(SavedTorrent {
        infohash,
        name,
        total_size: item.total_size,
        file_count: item.file_count,
        peers: item.peers,
        first_seen,
        last_seen,
    })
}

/// 合并同一条目的两份数据：补缺失字段、扩展时间窗、取较大 peer 数。
fn merge_saved(existing: &mut SavedTorrent, incoming: SavedTorrent) {
    if existing.total_size.is_none() {
        existing.total_size = incoming.total_size;
    }
    if existing.file_count.is_none() {
        existing.file_count = incoming.file_count;
    }
    existing.peers = existing.peers.max(incoming.peers);
    if incoming.first_seen > 0
        && (existing.first_seen <= 0 || incoming.first_seen < existing.first_seen)
    {
        existing.first_seen = incoming.first_seen;
    }
    existing.last_seen = existing.last_seen.max(incoming.last_seen);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht_radar::store::hex;

    fn saved(seed: u8, name: &str, last_seen: i64) -> SavedTorrent {
        SavedTorrent {
            infohash: [seed; 20],
            name: name.to_owned(),
            total_size: Some(1024),
            file_count: Some(1),
            peers: 2,
            first_seen: 10,
            last_seen,
        }
    }

    #[test]
    fn export_round_trips_through_import() {
        let rows = vec![saved(1, "debian.iso", 200), saved(2, "ubuntu.iso", 100)];
        let text = encode_export(&rows, 500).expect("encode");
        let parsed = parse_import(&text).expect("parse");
        assert_eq!(parsed.total, 2);
        assert_eq!(parsed.invalid, 0);
        assert_eq!(parsed.rows.len(), 2);
        // 最近观测在前。
        assert_eq!(parsed.rows[0].name, "debian.iso");
        assert_eq!(parsed.rows[1].name, "ubuntu.iso");
    }

    #[test]
    fn import_dedupes_by_infohash_and_merges_fields() {
        let text = format!(
            r#"{{"version":1,"items":[
                {{"infoHash":"{hash}","name":"first.iso","peers":1,"firstSeen":300,"lastSeen":300}},
                {{"infoHash":"{hash}","name":"second.iso","totalSize":42,"peers":9,"firstSeen":100,"lastSeen":400}}
            ]}}"#,
            hash = "ab".repeat(20)
        );
        let parsed = parse_import(&text).expect("parse");
        assert_eq!(parsed.total, 2);
        assert_eq!(parsed.rows.len(), 1);
        let row = &parsed.rows[0];
        assert_eq!(row.name, "first.iso", "先出现的名字保留");
        assert_eq!(row.total_size, Some(42), "缺失字段由第二条补上");
        assert_eq!(row.peers, 9);
        assert_eq!(row.first_seen, 100);
        assert_eq!(row.last_seen, 400);
    }

    #[test]
    fn import_accepts_plain_array_and_skips_invalid_items() {
        let text = format!(
            r#"[
                {{"infoHash":"{good}","name":"ok.iso"}},
                {{"infoHash":"not-a-hash","name":"bad.iso"}},
                {{"infoHash":"{good2}","name":"   "}},
                {{"infoHash":"{good2}","name":"size.iso","totalSize":-1}}
            ]"#,
            good = "cd".repeat(20),
            good2 = "ef".repeat(20)
        );
        let parsed = parse_import(&text).expect("parse");
        assert_eq!(parsed.rows.len(), 1);
        assert_eq!(parsed.rows[0].name, "ok.iso");
        assert_eq!(parsed.invalid, 3);
    }

    #[test]
    fn import_rejects_newer_version() {
        let error = parse_import(r#"{"version":99,"items":[]}"#).expect_err("version");
        assert!(matches!(
            error,
            ImportError::UnsupportedVersion {
                found: 99,
                supported: EXPORT_VERSION
            }
        ));
    }

    #[test]
    fn import_rejects_invalid_json() {
        assert!(matches!(
            parse_import("not json"),
            Err(ImportError::Json(_))
        ));
    }

    #[test]
    fn archive_writes_gzip_json_without_overwriting() {
        let dir = std::env::temp_dir().join(format!(
            "fluxdown_dht_radar_archive_{}",
            uuid::Uuid::new_v4()
        ));
        let rows = vec![saved(3, "arch.iso", 50)];
        let first = write_archive(&dir, &rows, 777).expect("first archive");
        let second = write_archive(&dir, &rows, 777).expect("second archive");
        assert_ne!(first, second);
        assert!(first.exists() && second.exists());

        // gzip 是二进制：用解码器读回，确认内容仍是可导入的导出文档。
        let bytes = std::fs::read(&first).expect("read archive");
        let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut json = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut json).expect("gunzip");
        let parsed = parse_import(&json).expect("parse archived json");
        assert_eq!(parsed.rows.len(), 1);
        assert_eq!(parsed.rows[0].name, "arch.iso");
        assert_eq!(hex(&parsed.rows[0].infohash), "03".repeat(20));

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
