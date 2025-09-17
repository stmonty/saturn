use crate::crc::*;
use crate::wal_format::*;

use std::io::{self, Write};

fn put_fixed32_le(dst4: &mut [u8], v: u32) {
    dst4[0] = (v & 0xFF) as u8;
    dst4[1] = ((v >> 8) & 0xFF) as u8;
    dst4[2] = ((v >> 16) & 0xFF) as u8;
    dst4[3] = ((v >> 24) & 0xFF) as u8;
}

pub struct Writer<W: Write> {
    dest: W,
    block_offset: usize,
    type_crc: [u32; MAX_RECORD_TYPE + 1],
}

impl<W: Write> Writer<W> {
    // For a brand-new empty stream (cursor already where you want to append).
    pub fn new(dest: W) -> Self {
        let mut w = Self {
            dest,
            block_offset: 0,
            type_crc: [0; MAX_RECORD_TYPE + 1],
        };
        for i in 0..=MAX_RECORD_TYPE {
            w.type_crc[i] = crc32c::value(&[i as u8]);
        }
        w
    }

    // If you’re appending to an existing file, pass its current length (or any offset mod block).
    pub fn with_starting_offset(dest: W, existing_len: u64) -> Self {
        let mut w = Self {
            dest,
            block_offset: (existing_len as usize) % BLOCK_SIZE,
            type_crc: [0; MAX_RECORD_TYPE + 1],
        };
        for i in 0..=MAX_RECORD_TYPE {
            w.type_crc[i] = crc32c::value(&[i as u8]);
        }
        w
    }

    pub fn add_record(&mut self, mut data: &[u8]) -> io::Result<()> {
        let mut begin = true;

        loop {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    // pad trailer with zeros
                    let zeros = vec![0u8; leftover];
                    self.dest.write_all(&zeros)?;
                    self.dest.flush()?;
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let frag_len = data.len().min(avail);
            let end = frag_len == data.len();

            let typ = match (begin, end) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            self.emit_physical_record(typ, &data[..frag_len])?;
            data = &data[frag_len..];
            begin = false;

            if data.is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn emit_physical_record(&mut self, t: RecordType, payload: &[u8]) -> io::Result<()> {
        let n = payload.len();
        let mut header = [0u8; HEADER_SIZE];
        header[4] = (n & 0xFF) as u8;
        header[5] = ((n >> 8) & 0xFF) as u8;
        header[6] = t as u8;

        let crc = crc32c::extend(self.type_crc[t as usize], payload);
        let masked = crc32c::mask(crc);
        put_fixed32_le(&mut header[0..4], masked);

        self.dest.write_all(&header)?;
        self.dest.write_all(payload)?;
        self.dest.flush()?; // matches LevelDB behavior
        self.block_offset += HEADER_SIZE + n;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.dest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Parse physical records from a raw log buffer (ignores CRC; skips trailers).
    fn parse_phys(buf: &[u8]) -> Vec<(u8 /*type*/, usize /*len*/, usize /*offset*/)> {
        let mut out = Vec::new();
        let mut i = 0usize;
        while i + HEADER_SIZE <= buf.len() {
            let block_off = i % BLOCK_SIZE;
            let room = BLOCK_SIZE - block_off;
            if room < HEADER_SIZE {
                // trailer region: skip to start of next block
                i += room;
                continue;
            }
            let header = &buf[i..i + HEADER_SIZE];
            // Decode length and type (little-endian length)
            let len = (header[4] as usize) | ((header[5] as usize) << 8);
            let typ = header[6];

            // If the full payload doesn't fit in the remainder of this slice, stop.
            if i + HEADER_SIZE + len > buf.len() {
                break;
            }

            // Good record
            out.push((typ, len, i));
            i += HEADER_SIZE + len;
        }
        out
    }

    #[test]
    fn write_two_full_records() {
        // Write two small records.
        let mut w = Writer::new(Vec::<u8>::new());
        w.add_record(b"hello").unwrap();
        w.add_record(b"world!").unwrap();
        let buf = w.into_inner();

        // Expect two FULL (type=1) physical records back-to-back.
        let recs = parse_phys(&buf);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].0, 1);
        assert_eq!(recs[0].1, 5);
        assert_eq!(recs[1].0, 1);
        assert_eq!(recs[1].1, 6);

        // Check payload bytes match.
        let h1 = recs[0].2;
        assert_eq!(&buf[h1 + HEADER_SIZE..h1 + HEADER_SIZE + 5], b"hello");
        let h2 = recs[1].2;
        assert_eq!(&buf[h2 + HEADER_SIZE..h2 + HEADER_SIZE + 6], b"world!");
    }

    #[test]
    fn pads_trailer_then_writes_next_block() {
        // Choose a payload that leaves < HEADER_SIZE bytes at end of the block.
        let pad_len = 3; // any 1..=6 works
        let first_len = BLOCK_SIZE - HEADER_SIZE - pad_len;
        let mut w = Writer::new(Vec::<u8>::new());
        w.add_record(&vec![b'x'; first_len]).unwrap();
        w.add_record(b"abc").unwrap();
        let buf = w.into_inner();

        // After the first record, there should be exactly `pad_len` zeros (trailer),
        // then the next record header at the start of the next block.
        let recs = parse_phys(&buf);
        assert!(recs.len() >= 2);
        let first_end = recs[0].2 + HEADER_SIZE + first_len;
        assert_eq!(&buf[first_end..first_end + pad_len], &[0u8; 3]);

        // The next record should be FULL "abc"
        let (typ2, len2, off2) = recs[1];
        assert_eq!(typ2, 1);
        assert_eq!(len2, 3);
        assert_eq!(&buf[off2 + HEADER_SIZE..off2 + HEADER_SIZE + 3], b"abc");
        // And it should start at a fresh block boundary.
        assert_eq!(off2 % BLOCK_SIZE, 0);
    }

    #[test]
    fn fragments_large_record_first_last() {
        // Make a single logical record slightly larger than one block's available payload.
        let avail = BLOCK_SIZE - HEADER_SIZE;
        let total = avail + 10;
        let mut w = Writer::new(Vec::<u8>::new());
        w.add_record(&vec![b'y'; total]).unwrap();
        let buf = w.into_inner();

        // Expect FIRST of length `avail`, then LAST of length 10.
        let recs = parse_phys(&buf);
        assert!(recs.len() >= 2);
        assert_eq!(recs[0].0, 2); // FIRST
        assert_eq!(recs[0].1, avail);
        assert_eq!(recs[1].0, 4); // LAST
        assert_eq!(recs[1].1, 10);
    }
}
