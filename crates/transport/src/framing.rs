//! Length-prefixed framing: `[4-byte big-endian length][payload]`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::TransportError;

#[derive(Debug, Clone)]
pub struct LengthPrefixCodec {
    max_message_size: usize,
}

impl LengthPrefixCodec {
    pub fn new(max_message_size: usize) -> Self {
        Self { max_message_size }
    }

    pub fn encode(&self, payload: &[u8]) -> Result<Bytes, TransportError> {
        if payload.len() > self.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: payload.len(),
                max: self.max_message_size,
            });
        }
        let mut buf = BytesMut::with_capacity(4 + payload.len());
        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
        Ok(buf.freeze())
    }

    pub fn decode_length(&self, header: &[u8; 4]) -> Result<usize, TransportError> {
        let len = u32::from_be_bytes(*header) as usize;
        if len > self.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: len,
                max: self.max_message_size,
            });
        }
        Ok(len)
    }
}

impl Default for LengthPrefixCodec {
    fn default() -> Self {
        Self::new(4 * 1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let codec = LengthPrefixCodec::default();
        let payload = b"hello world";
        let frame = codec.encode(payload).unwrap();

        assert_eq!(&frame[..4], &(payload.len() as u32).to_be_bytes());
        assert_eq!(&frame[4..], payload);
    }

    #[test]
    fn rejects_oversized() {
        let codec = LengthPrefixCodec::new(10);
        let payload = vec![0u8; 11];
        assert!(codec.encode(&payload).is_err());
    }
}
