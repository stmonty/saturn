pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type SequenceNumber = u64;

#[derive(Debug, Clone)]
pub struct SegmentHandle {
    offset: usize,
    length: usize,
}

pub fn encode_var(mut n: usize, dest: &mut [u8]) -> usize {
    let mut i = 0;
    loop {
        let mut byte = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        if i < dest.len() {
            dest[i] = byte;
            i += 1;
        } else {
            return 0;
        }
        if n == 0 {
            break;
        }
    }
    i
}

/// Returns the number of bytes required to encode `n` as a LEB128/varint.
pub fn required_space(mut n: usize) -> usize {
    let mut bytes = 1;
    while n >= 0x80 {
        n >>= 7;
        bytes += 1;
    }
    bytes
}

pub fn decode_var(src: &[u8]) -> Option<(usize, usize)> {
    let mut result: usize = 0;
    let mut shift = 0;
    for (i, &byte) in src.iter().enumerate() {
        result |= ((byte & 0x7F) as usize) << shift;
        if (byte & 0x80) == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift > usize::BITS as usize {
            return None;
        }
    }
    None
}

impl SegmentHandle {
    pub fn new(offset: usize, length: usize) -> Self {
        SegmentHandle {
            offset: offset,
            length: length,
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn length(&self) -> usize {
        self.length
    }

    /// Encodes the SegmentHandle as varint into `dest`.
    /// Returns bytes written or 0 if `dest` is too small.
    pub fn encode(&self, dest: &mut [u8]) -> usize {
        if dest.len() < required_space(self.offset) + required_space(self.length) {
            return 0;
        }
        let offset = encode_var(self.offset, dest);
        offset + encode_var(self.length, &mut dest[offset..])
    }

    /// Decodes a SegmentHandle from a varint-encoded byte slice.
    /// Returns (SegmentHandle, bytes_read) or None on failure.
    pub fn decode(from: &[u8]) -> Option<(SegmentHandle, usize)> {
        let (offset, offset_len) = decode_var(from)?;
        let (length, length_len) = decode_var(&from[offset_len..])?;

        Some((SegmentHandle { offset, length }, offset_len + length_len))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Put { key: Key, value: Value },
    Delete { key: Key },
}
