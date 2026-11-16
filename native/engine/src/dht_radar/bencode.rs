//! 最小 bencode 编解码（BEP 3）。
//!
//! DHT 的 KRPC 报文与种子元数据都是 bencode，但工作区里没有现成的实现，且该格式足够小，
//! 因此这里自带一份：只做编解码与取值，不做 schema 校验。解码带长度上限与深度上限，
//! 避免对端用超长字符串或超深嵌套耗尽内存。

use std::collections::BTreeMap;
use std::fmt;

/// bencode 值。字典用 `BTreeMap` 保证键有序（编码结果与规范一致）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

impl Value {
    /// 构造字节串值。
    pub fn bytes(value: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(value.into())
    }

    /// 构造字典值。
    pub fn dict(entries: impl IntoIterator<Item = (impl Into<Vec<u8>>, Value)>) -> Self {
        Self::Dict(entries.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }

    /// 取整数值。
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    /// 取字节串值。
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }

    /// 取列表值。
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(value) => Some(value),
            _ => None,
        }
    }

    /// 取字典值。
    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Self::Dict(value) => Some(value),
            _ => None,
        }
    }

    /// 按键取字典成员。
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.as_dict().and_then(|dict| dict.get(key))
    }

    /// 按路径取成员，任意一层缺失即为 `None`。
    pub fn path(&self, path: &[&[u8]]) -> Option<&Value> {
        let mut current = self;
        for key in path {
            current = current.get(key)?;
        }
        Some(current)
    }

    /// 取字典成员的整数值。
    pub fn get_int(&self, key: &[u8]) -> Option<i64> {
        self.get(key).and_then(Value::as_int)
    }

    /// 取字典成员的字节串值。
    pub fn get_bytes(&self, key: &[u8]) -> Option<&[u8]> {
        self.get(key).and_then(Value::as_bytes)
    }
}

/// 解码失败原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// 报文被截断。
    Truncated,
    /// 起始字节不是合法类型。
    UnexpectedType(u8),
    /// 数字格式非法。
    InvalidNumber,
    /// 字符串长度前缀非法或超出剩余长度。
    InvalidLength,
    /// 嵌套层数超过上限。
    TooDeep,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("bencode payload truncated"),
            Self::UnexpectedType(byte) => {
                write!(formatter, "unexpected bencode type byte {byte:#x}")
            }
            Self::InvalidNumber => formatter.write_str("invalid bencode integer"),
            Self::InvalidLength => formatter.write_str("invalid bencode string length"),
            Self::TooDeep => formatter.write_str("bencode nesting too deep"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// 单个字节串的长度上限（元数据分片 16 KiB，留足余量）。
const MAX_STRING_BYTES: usize = 4 * 1024 * 1024;
/// 嵌套深度上限。
const MAX_DEPTH: usize = 32;

/// 编码为 bencode 字节串。
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    encode_into(value, &mut out);
    out
}

fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(number) => {
            out.push(b'i');
            out.extend_from_slice(number.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(bytes) => {
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(bytes);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(entries) => {
            out.push(b'd');
            for (key, item) in entries {
                encode_into(&Value::Bytes(key.clone()), out);
                encode_into(item, out);
            }
            out.push(b'e');
        }
    }
}

/// 解码整段字节串；尾部存在多余内容视为失败。
pub fn decode(input: &[u8]) -> Result<Value, DecodeError> {
    let (value, used) = decode_prefix(input)?;
    if used != input.len() {
        return Err(DecodeError::InvalidLength);
    }
    Ok(value)
}

/// 解码开头的一个值，返回值与消耗的字节数（元数据分片是「头部 + 原始分片」，需要这个）。
pub fn decode_prefix(input: &[u8]) -> Result<(Value, usize), DecodeError> {
    let mut cursor = 0;
    let value = decode_value(input, &mut cursor, 0)?;
    Ok((value, cursor))
}

fn decode_value(input: &[u8], cursor: &mut usize, depth: usize) -> Result<Value, DecodeError> {
    if depth > MAX_DEPTH {
        return Err(DecodeError::TooDeep);
    }
    let byte = *input.get(*cursor).ok_or(DecodeError::Truncated)?;
    match byte {
        b'i' => {
            *cursor += 1;
            let end = input[*cursor..]
                .iter()
                .position(|byte| *byte == b'e')
                .ok_or(DecodeError::Truncated)?
                + *cursor;
            let text = std::str::from_utf8(&input[*cursor..end])
                .map_err(|_| DecodeError::InvalidNumber)?;
            let number = text
                .parse::<i64>()
                .map_err(|_| DecodeError::InvalidNumber)?;
            *cursor = end + 1;
            Ok(Value::Int(number))
        }
        b'l' => {
            *cursor += 1;
            let mut items = Vec::new();
            loop {
                match input.get(*cursor) {
                    Some(b'e') => {
                        *cursor += 1;
                        return Ok(Value::List(items));
                    }
                    Some(_) => items.push(decode_value(input, cursor, depth + 1)?),
                    None => return Err(DecodeError::Truncated),
                }
            }
        }
        b'd' => {
            *cursor += 1;
            let mut entries = BTreeMap::new();
            loop {
                match input.get(*cursor) {
                    Some(b'e') => {
                        *cursor += 1;
                        return Ok(Value::Dict(entries));
                    }
                    Some(_) => {
                        let key = decode_bytes(input, cursor)?;
                        let value = decode_value(input, cursor, depth + 1)?;
                        entries.insert(key, value);
                    }
                    None => return Err(DecodeError::Truncated),
                }
            }
        }
        b'0'..=b'9' => {
            let bytes = decode_bytes(input, cursor)?;
            Ok(Value::Bytes(bytes))
        }
        other => Err(DecodeError::UnexpectedType(other)),
    }
}

fn decode_bytes(input: &[u8], cursor: &mut usize) -> Result<Vec<u8>, DecodeError> {
    let start = *cursor;
    let colon = input[start..]
        .iter()
        .position(|byte| *byte == b':')
        .ok_or(DecodeError::Truncated)?
        + start;
    let text = std::str::from_utf8(&input[start..colon]).map_err(|_| DecodeError::InvalidLength)?;
    let length: usize = text.parse().map_err(|_| DecodeError::InvalidLength)?;
    if length > MAX_STRING_BYTES {
        return Err(DecodeError::InvalidLength);
    }
    let from = colon + 1;
    let to = from.checked_add(length).ok_or(DecodeError::InvalidLength)?;
    if to > input.len() {
        return Err(DecodeError::Truncated);
    }
    *cursor = to;
    Ok(input[from..to].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: Value) {
        let encoded = encode(&value);
        let decoded = decode(&encoded).expect("decodes");
        assert_eq!(decoded, value);
    }

    #[test]
    fn integers_round_trip() {
        for number in [0, 1, -1, 42, i64::from(i32::MAX), -i64::from(i32::MAX)] {
            round_trip(Value::Int(number));
        }
    }

    #[test]
    fn strings_lists_and_dicts_round_trip() {
        round_trip(Value::bytes(&b"hello"[..]));
        round_trip(Value::List(vec![
            Value::Int(1),
            Value::bytes(&b"a"[..]),
            Value::List(vec![Value::Int(2)]),
        ]));
        round_trip(Value::dict([
            (
                &b"m"[..],
                Value::dict([(&b"ut_metadata"[..], Value::Int(1))]),
            ),
            (&b"metadata_size"[..], Value::Int(1234)),
        ]));
    }

    #[test]
    fn dict_keys_are_sorted_on_encode() {
        let value = Value::dict([(&b"z"[..], Value::Int(1)), (&b"a"[..], Value::Int(2))]);
        assert_eq!(encode(&value), b"d1:ai2e1:zi1ee".to_vec());
    }

    #[test]
    fn decode_prefix_reports_consumed_bytes() {
        let mut payload = encode(&Value::dict([(&b"msg_type"[..], Value::Int(1))]));
        let header_len = payload.len();
        payload.extend_from_slice(&[0xff, 0x00, 0x11]);
        let (value, used) = decode_prefix(&payload).expect("decodes");
        assert_eq!(used, header_len);
        assert_eq!(value.get_int(b"msg_type"), Some(1));
    }

    #[test]
    fn truncated_payload_is_rejected() {
        assert_eq!(decode(b"d1:a"), Err(DecodeError::Truncated));
        assert_eq!(decode(b"i12"), Err(DecodeError::Truncated));
        assert_eq!(decode(b"5:abc"), Err(DecodeError::Truncated));
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        assert_eq!(decode(b"i1ee"), Err(DecodeError::InvalidLength));
    }

    #[test]
    fn unexpected_type_byte_is_rejected() {
        assert_eq!(decode(b"x"), Err(DecodeError::UnexpectedType(b'x')));
    }
}
