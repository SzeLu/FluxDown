//! KRPC 报文（BEP 5）的构造与解析。
//!
//! 只覆盖 DHT 爬虫用到的四种查询（`ping` / `find_node` / `get_peers` / `announce_peer`）
//! 与三种应答（`r` / `e` / 查询本身），节点表使用 26 字节紧凑格式。

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use rand::RngCore;

pub use super::bencode::Value;
use super::bencode::{self, DecodeError};

/// 节点 ID 长度。
pub const NODE_ID_LEN: usize = 20;
/// 紧凑节点条目长度：20 字节 ID + 4 字节 IPv4 + 2 字节端口。
const COMPACT_NODE_LEN: usize = 26;

/// DHT 节点 ID。
pub type NodeId = [u8; NODE_ID_LEN];

/// 随机节点 ID。
pub fn random_node_id() -> NodeId {
    let mut id = [0u8; NODE_ID_LEN];
    rand::rng().fill_bytes(&mut id);
    id
}

/// 事务 ID：两字节大端，请求与应答靠它配对。
pub fn transaction_id(value: u16) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// 构造查询报文。
pub fn query(transaction: u16, name: &str, args: Value) -> Vec<u8> {
    bencode::encode(&Value::dict([
        (&b"t"[..], Value::Bytes(transaction_id(transaction))),
        (&b"y"[..], Value::bytes(&b"q"[..])),
        (&b"q"[..], Value::bytes(name.as_bytes().to_vec())),
        (&b"a"[..], args),
    ]))
}

/// 构造正常应答报文。
pub fn response(transaction: u16, values: Value) -> Vec<u8> {
    bencode::encode(&Value::dict([
        (&b"t"[..], Value::Bytes(transaction_id(transaction))),
        (&b"y"[..], Value::bytes(&b"r"[..])),
        (&b"r"[..], values),
    ]))
}

/// 构造错误应答报文。
pub fn error_response(transaction: u16, code: i64, message: &str) -> Vec<u8> {
    bencode::encode(&Value::dict([
        (&b"t"[..], Value::Bytes(transaction_id(transaction))),
        (&b"y"[..], Value::bytes(&b"e"[..])),
        (
            &b"e"[..],
            Value::List(vec![
                Value::Int(code),
                Value::bytes(message.as_bytes().to_vec()),
            ]),
        ),
    ]))
}

/// 解析报文。
pub fn parse(payload: &[u8]) -> Result<Value, DecodeError> {
    bencode::decode(payload)
}

/// 报文类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// 查询（`y=q`）。
    Query,
    /// 应答（`y=r`）。
    Response,
    /// 错误（`y=e`）。
    Error,
}

/// 报文类型判定。
pub fn kind(message: &Value) -> Option<MessageKind> {
    match message.get_bytes(b"y")? {
        b"q" => Some(MessageKind::Query),
        b"r" => Some(MessageKind::Response),
        b"e" => Some(MessageKind::Error),
        _ => None,
    }
}

/// 事务 ID（两字节大端）。
pub fn transaction(message: &Value) -> Option<u16> {
    let bytes = message.get_bytes(b"t")?;
    let pair: [u8; 2] = bytes.try_into().ok()?;
    Some(u16::from_be_bytes(pair))
}

/// 查询名（`q`）。
pub fn query_name(message: &Value) -> Option<&str> {
    std::str::from_utf8(message.get_bytes(b"q")?).ok()
}

/// 查询参数（`a`）。
pub fn query_args(message: &Value) -> Option<&Value> {
    message.get(b"a")
}

/// 应答体（`r`）。
pub fn response_body(message: &Value) -> Option<&Value> {
    message.get(b"r")
}

/// 从查询参数里取节点 ID。
pub fn sender_id(args: &Value) -> Option<NodeId> {
    let bytes = args.get_bytes(b"id")?;
    bytes.try_into().ok()
}

/// 紧凑节点列表编码。
pub fn encode_nodes(nodes: &[(NodeId, SocketAddrV4)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nodes.len() * COMPACT_NODE_LEN);
    for (id, address) in nodes {
        out.extend_from_slice(id);
        out.extend_from_slice(&address.ip().octets());
        out.extend_from_slice(&address.port().to_be_bytes());
    }
    out
}

/// 紧凑节点列表解码；长度不是 26 的整数倍时截断到最近的完整条目。
pub fn decode_nodes(bytes: &[u8]) -> Vec<(NodeId, SocketAddrV4)> {
    bytes
        .as_chunks::<COMPACT_NODE_LEN>()
        .0
        .iter()
        .filter_map(|chunk| {
            let mut id = [0u8; NODE_ID_LEN];
            id.copy_from_slice(&chunk[..NODE_ID_LEN]);
            let octets: [u8; 4] = chunk[NODE_ID_LEN..NODE_ID_LEN + 4].try_into().ok()?;
            let port =
                u16::from_be_bytes(chunk[NODE_ID_LEN + 4..COMPACT_NODE_LEN].try_into().ok()?);
            if port == 0 {
                return None;
            }
            Some((id, SocketAddrV4::new(Ipv4Addr::from(octets), port)))
        })
        .collect()
}

/// `get_peers` 应答里的 peer 列表（`values`）：紧凑 6 字节条目，忽略 IPv6（`values6`）。
pub fn decode_peers(body: &Value) -> Vec<SocketAddrV4> {
    let Some(values) = body.get(b"values").and_then(Value::as_list) else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(Value::as_bytes)
        .filter_map(|bytes| {
            let octets: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
            let port = u16::from_be_bytes(bytes.get(4..6)?.try_into().ok()?);
            (port > 0).then(|| SocketAddrV4::new(Ipv4Addr::from(octets), port))
        })
        .collect()
}

/// `sample_infohashes` 应答里的样本（BEP 51）：每 20 字节一个 infohash。
pub fn decode_samples(bytes: &[u8]) -> Vec<NodeId> {
    bytes
        .as_chunks::<NODE_ID_LEN>()
        .0
        .iter()
        .map(|chunk| {
            let mut hash = [0u8; NODE_ID_LEN];
            hash.copy_from_slice(chunk);
            hash
        })
        .collect()
}

/// 为来源地址签发 token：`announce_peer` 回环校验用，5 分钟内有效。
pub fn token_for(secret: &[u8; 16], address: SocketAddr) -> Vec<u8> {
    use sha1::Digest as _;
    let mut hasher = sha1::Sha1::new();
    hasher.update(secret);
    match address.ip() {
        std::net::IpAddr::V4(ip) => hasher.update(ip.octets()),
        std::net::IpAddr::V6(ip) => hasher.update(ip.octets()),
    }
    hasher.finalize()[..4].to_vec()
}

/// 组装 `find_node` / `get_peers` 共用的参数字典。
pub fn args_with_id(id: &NodeId) -> BTreeMap<Vec<u8>, Value> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(id.to_vec()));
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_round_trips_through_parser() {
        let id = random_node_id();
        let args = Value::Dict(args_with_id(&id));
        let payload = query(7, "find_node", args);
        let message = parse(&payload).expect("parses");
        assert_eq!(kind(&message), Some(MessageKind::Query));
        assert_eq!(transaction(&message), Some(7));
        assert_eq!(query_name(&message), Some("find_node"));
        assert_eq!(query_args(&message).and_then(sender_id).as_ref(), Some(&id));
    }

    #[test]
    fn response_carries_nodes() {
        let nodes = vec![
            (
                random_node_id(),
                SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881),
            ),
            (
                random_node_id(),
                SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 51413),
            ),
        ];
        let payload = response(
            1,
            Value::dict([(&b"nodes"[..], Value::Bytes(encode_nodes(&nodes)))]),
        );
        let message = parse(&payload).expect("parses");
        let body = response_body(&message).expect("body");
        let decoded = decode_nodes(body.get_bytes(b"nodes").expect("nodes"));
        assert_eq!(decoded, nodes);
    }

    #[test]
    fn compact_nodes_ignore_trailing_partial_entry() {
        let nodes = vec![(
            random_node_id(),
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6881),
        )];
        let mut bytes = encode_nodes(&nodes);
        bytes.extend_from_slice(&[0u8; 5]);
        assert_eq!(decode_nodes(&bytes), nodes);
    }

    #[test]
    fn compact_nodes_drop_zero_port() {
        let mut bytes = random_node_id().to_vec();
        bytes.extend_from_slice(&[1, 2, 3, 4, 0, 0]);
        assert!(decode_nodes(&bytes).is_empty());
    }

    #[test]
    fn peer_values_decode() {
        let body = Value::dict([(
            &b"values"[..],
            Value::List(vec![
                Value::Bytes(vec![1, 2, 3, 4, 0x1a, 0xe1]),
                Value::Bytes(vec![1, 2, 3, 4, 0, 0]),
            ]),
        )]);
        let peers = decode_peers(&body);
        assert_eq!(
            peers,
            vec![SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881)]
        );
    }

    #[test]
    fn tokens_are_stable_per_address() {
        let secret = [9u8; 16];
        let first = token_for(&secret, "1.2.3.4:6881".parse().expect("addr"));
        let second = token_for(&secret, "1.2.3.4:9999".parse().expect("addr"));
        assert_eq!(first, second);
        assert_ne!(
            first,
            token_for(&secret, "5.6.7.8:6881".parse().expect("addr"))
        );
        assert_eq!(first.len(), 4);
    }

    #[test]
    fn error_response_has_code_and_message() {
        let payload = error_response(3, 203, "Protocol Error");
        let message = parse(&payload).expect("parses");
        assert_eq!(kind(&message), Some(MessageKind::Error));
        let body = message.get(b"e").and_then(Value::as_list).expect("list");
        assert_eq!(body[0].as_int(), Some(203));
        assert_eq!(body[1].as_bytes(), Some(&b"Protocol Error"[..]));
    }
}
