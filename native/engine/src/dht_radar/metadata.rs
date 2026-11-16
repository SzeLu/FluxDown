//! BEP 9 / BEP 10：向 peer 拉取种子元数据（`info` 字典）。
//!
//! 采集线程拿到的是 infohash 和来源 peer 地址；这里用一次短连接完成
//! 「扩展握手 → 请求分片 → 拼装 → SHA-1 校验 → 解析名称与体积」。
//! 全部是阻塞 IO 且带超时，调用方放在独立线程里跑，不占异步运行时。

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use sha1::Digest as _;

use super::bencode::{self, Value};
use super::krpc::NodeId;
use super::store::MetadataSummary;

/// BEP 9 分片大小。
const PIECE_SIZE: usize = 16 * 1024;
/// 元数据体积上限（超过视为异常，直接放弃）。
const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
/// 建立连接超时。DHT 里大量 peer 地址已下线，握手阶段快失败才能把并发留给活地址。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// 单次读写超时。
const IO_TIMEOUT: Duration = Duration::from_secs(4);
/// 整个抓取流程的时间上限。
const FETCH_BUDGET: Duration = Duration::from_secs(10);
/// 我们向对端公布的 `ut_metadata` 扩展 ID。
const OUR_UT_METADATA_ID: u8 = 1;
/// 扩展协议消息 ID。
const EXTENDED_MESSAGE_ID: u8 = 20;

/// 抓取失败原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    /// 连接失败。
    Connect,
    /// 握手失败（协议不符、infohash 不匹配或对端不支持扩展协议）。
    Handshake,
    /// 对端不支持 `ut_metadata`。
    Unsupported,
    /// 分片请求/应答失败。
    Transfer,
    /// SHA-1 与 infohash 不一致。
    HashMismatch,
    /// 元数据体积非法或 `info` 字典解析失败。
    InvalidMetadata,
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Connect => "peer connect failed",
            Self::Handshake => "peer handshake failed",
            Self::Unsupported => "peer does not support ut_metadata",
            Self::Transfer => "metadata transfer failed",
            Self::HashMismatch => "metadata hash mismatch",
            Self::InvalidMetadata => "metadata payload invalid",
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for MetadataError {}

/// 从 peer 拉取并解析元数据。
pub fn fetch(
    infohash: &[u8; 20],
    peer: SocketAddr,
    peer_id: &NodeId,
) -> Result<MetadataSummary, MetadataError> {
    let deadline = Instant::now() + FETCH_BUDGET;
    let mut stream =
        TcpStream::connect_timeout(&peer, CONNECT_TIMEOUT).map_err(|_| MetadataError::Connect)?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|_| MetadataError::Connect)?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|_| MetadataError::Connect)?;
    stream
        .set_nodelay(true)
        .map_err(|_| MetadataError::Connect)?;

    handshake(&mut stream, infohash, peer_id)?;
    let (peer_ut_metadata, total_size) = extended_handshake(&mut stream)?;
    let info = download_pieces(&mut stream, peer_ut_metadata, total_size, deadline)?;
    if sha1_of(&info) != *infohash {
        return Err(MetadataError::HashMismatch);
    }
    let dict = bencode::decode(&info).map_err(|_| MetadataError::InvalidMetadata)?;
    Ok(summarize(&dict))
}

fn sha1_of(bytes: &[u8]) -> [u8; 20] {
    let mut hasher = sha1::Sha1::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

fn handshake(
    stream: &mut TcpStream,
    infohash: &[u8; 20],
    peer_id: &NodeId,
) -> Result<(), MetadataError> {
    let mut out = Vec::with_capacity(68);
    out.push(19);
    out.extend_from_slice(b"BitTorrent protocol");
    // 只置扩展协议位（reserved[5] 的 0x10），其余保持 0。
    let mut reserved = [0u8; 8];
    reserved[5] = 0x10;
    out.extend_from_slice(&reserved);
    out.extend_from_slice(infohash);
    out.extend_from_slice(peer_id);
    stream
        .write_all(&out)
        .map_err(|_| MetadataError::Handshake)?;

    let mut reply = [0u8; 68];
    stream
        .read_exact(&mut reply)
        .map_err(|_| MetadataError::Handshake)?;
    if reply[0] != 19 || &reply[1..20] != b"BitTorrent protocol" {
        return Err(MetadataError::Handshake);
    }
    if &reply[28..48] != infohash {
        return Err(MetadataError::Handshake);
    }
    if reply[25] & 0x10 == 0 {
        return Err(MetadataError::Unsupported);
    }
    Ok(())
}

/// 发送我们的扩展握手，读取对端的扩展握手，返回 `(对端 ut_metadata ID, 元数据总长度)`。
fn extended_handshake(stream: &mut TcpStream) -> Result<(u8, usize), MetadataError> {
    let payload = bencode::encode(&Value::dict([(
        &b"m"[..],
        Value::dict([(
            &b"ut_metadata"[..],
            Value::Int(i64::from(OUR_UT_METADATA_ID)),
        )]),
    )]));
    send_extended(stream, 0, &payload)?;

    loop {
        let Some((extension, body)) = read_message(stream)? else {
            continue;
        };
        if extension != 0 {
            continue;
        }
        let dict = bencode::decode(&body).map_err(|_| MetadataError::Transfer)?;
        let peer_id = dict
            .path(&[b"m", b"ut_metadata"])
            .and_then(Value::as_int)
            .ok_or(MetadataError::Unsupported)?;
        let total = dict
            .get_int(b"metadata_size")
            .ok_or(MetadataError::Unsupported)?;
        let total = usize::try_from(total).map_err(|_| MetadataError::InvalidMetadata)?;
        if total == 0 || total > MAX_METADATA_BYTES {
            return Err(MetadataError::InvalidMetadata);
        }
        let peer_id = u8::try_from(peer_id).map_err(|_| MetadataError::Unsupported)?;
        return Ok((peer_id, total));
    }
}

fn download_pieces(
    stream: &mut TcpStream,
    peer_ut_metadata: u8,
    total_size: usize,
    deadline: Instant,
) -> Result<Vec<u8>, MetadataError> {
    let count = total_size.div_ceil(PIECE_SIZE);
    let mut pieces: Vec<Option<Vec<u8>>> = vec![None; count];
    let mut received = 0usize;
    let mut requested = 0usize;

    while received < count {
        if Instant::now() >= deadline {
            return Err(MetadataError::Transfer);
        }
        // 先要两片，之后每收到一片再补一片，避免一次灌满对端的发送队列。
        while requested < count && requested < received + 2 {
            let request = bencode::encode(&Value::dict([
                (&b"msg_type"[..], Value::Int(0)),
                (&b"piece"[..], Value::Int(requested as i64)),
            ]));
            send_extended(stream, peer_ut_metadata, &request)?;
            requested += 1;
        }

        let Some((extension, body)) = read_message(stream)? else {
            continue;
        };
        if extension != OUR_UT_METADATA_ID {
            continue;
        }
        let (header, consumed) =
            bencode::decode_prefix(&body).map_err(|_| MetadataError::Transfer)?;
        let message_type = header.get_int(b"msg_type").unwrap_or(-1);
        if message_type == 2 {
            // 对端拒绝该分片（`reject`）。
            return Err(MetadataError::Transfer);
        }
        if message_type != 1 {
            continue;
        }
        let index = header
            .get_int(b"piece")
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(MetadataError::Transfer)?;
        if index >= count || pieces[index].is_some() {
            continue;
        }
        let data = &body[consumed..];
        let expected = PIECE_SIZE.min(total_size - index * PIECE_SIZE);
        if data.len() < expected {
            return Err(MetadataError::Transfer);
        }
        pieces[index] = Some(data[..expected].to_vec());
        received += 1;
    }

    let mut info = Vec::with_capacity(total_size);
    for piece in pieces {
        info.extend_from_slice(&piece.ok_or(MetadataError::Transfer)?);
    }
    Ok(info)
}

fn send_extended(
    stream: &mut TcpStream,
    extension: u8,
    payload: &[u8],
) -> Result<(), MetadataError> {
    let length = u32::try_from(payload.len() + 2).map_err(|_| MetadataError::Transfer)?;
    let mut out = Vec::with_capacity(payload.len() + 6);
    out.extend_from_slice(&length.to_be_bytes());
    out.push(EXTENDED_MESSAGE_ID);
    out.push(extension);
    out.extend_from_slice(payload);
    stream.write_all(&out).map_err(|_| MetadataError::Transfer)
}

/// 读一条消息；非扩展协议消息（bitfield / have / choke 等）返回 `None` 由调用方跳过。
///
/// 握手后对端通常先发 bitfield，把它当成错误会白白丢掉绝大多数本可成功的对端。
fn read_message(stream: &mut TcpStream) -> Result<Option<(u8, Vec<u8>)>, MetadataError> {
    let mut length_bytes = [0u8; 4];
    stream
        .read_exact(&mut length_bytes)
        .map_err(|_| MetadataError::Transfer)?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if !(2..=PIECE_SIZE + 1024).contains(&length) {
        return Err(MetadataError::Transfer);
    }
    let mut frame = vec![0u8; length];
    stream
        .read_exact(&mut frame)
        .map_err(|_| MetadataError::Transfer)?;
    if frame[0] != EXTENDED_MESSAGE_ID {
        return Ok(None);
    }
    Ok(Some((frame[1], frame[2..].to_vec())))
}

/// 展示用名称的最大字符数（超长的多是刻意塞满 `name` 的异常种子）。
const MAX_NAME_CHARS: usize = 200;

/// 解析 `info` 字典，取出名称、总体积与文件数。
pub fn summarize(info: &Value) -> MetadataSummary {
    let files = info.get(b"files").and_then(Value::as_list);
    // 有些种子不带 `name`（或只有空白），退一步用第一个文件的顶层目录名。
    let name = info
        .get_bytes(b"name.utf-8")
        .or_else(|| info.get_bytes(b"name"))
        .and_then(clean_name)
        .or_else(|| files.and_then(first_path_head).and_then(clean_name));
    if let Some(length) = info.get_int(b"length") {
        return MetadataSummary {
            name,
            total_size: u64::try_from(length).ok(),
            file_count: Some(1),
        };
    }
    let Some(files) = files else {
        return MetadataSummary {
            name,
            total_size: None,
            file_count: None,
        };
    };
    let total: u64 = files
        .iter()
        .filter_map(|file| file.get_int(b"length"))
        .filter_map(|length| u64::try_from(length).ok())
        .sum();
    MetadataSummary {
        name,
        total_size: (total > 0).then_some(total),
        file_count: u32::try_from(files.len()).ok(),
    }
}

/// 第一个文件路径的第一段（多文件种子的顶层目录名）。
fn first_path_head(files: &[Value]) -> Option<&[u8]> {
    let file = files.first()?;
    file.get(b"path.utf-8")
        .or_else(|| file.get(b"path"))
        .and_then(Value::as_list)?
        .first()?
        .as_bytes()
}

/// 把元数据里的原始名字清成可展示的一行：控制字符与多余空白折叠，超长截断。
fn clean_name(raw: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(raw);
    let mut out = String::with_capacity(text.len().min(MAX_NAME_CHARS * 4));
    let mut chars = 0usize;
    let mut pending_space = false;
    for ch in text.chars() {
        if ch.is_whitespace() || ch.is_control() {
            pending_space = !out.is_empty();
            continue;
        }
        if chars >= MAX_NAME_CHARS {
            break;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
            chars += 1;
        }
        out.push(ch);
        chars += 1;
    }
    let trimmed = out.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}
