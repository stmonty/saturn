use crate::crc::*;
use crate::wal_format::*;
use std::io::{self, Read, Seek, SeekFrom};

pub trait Reporter {
    fn corruption(&mut self, bytes: usize, reason: &str);
}

struct Buf {
    data: Vec<u8>,
    pos: usize,
}
impl Buf {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            pos: 0,
        }
    }
    fn size(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }
    fn as_slice(&self) -> &[u8] {
        &self.data[self.pos..]
    }
    fn clear(&mut self) {
        self.data.clear();
        self.pos = 0;
    }
    fn reset_to(&mut self, bytes: &[u8]) {
        self.data.clear();
        self.data.extend_from_slice(bytes);
        self.pos = 0;
    }
    fn remove_prefix(&mut self, n: usize) {
        self.pos += n;
        if self.pos >= self.data.len() {
            self.clear();
        }
    }
}

pub struct Reader<R: Read + Seek, Rep: Reporter> {
    src: R,
    reporter: Option<Rep>,
    checksum: bool,

    backing: Vec<u8>,
    buf: Buf,
    eof: bool,

    last_record_offset: u64,
    end_of_buffer_offset: u64,
    initial_offset: u64,
    resyncing: bool,
}

impl<R: Read + Seek, Rep: Reporter> Reader<R, Rep> {
    pub fn new(src: R, reporter: Option<Rep>, checksum: bool, initial_offset: u64) -> Self {
        Self {
            src,
            reporter,
            checksum,
            backing: vec![0u8; BLOCK_SIZE],
            buf: Buf::new(),
            eof: false,
            last_record_offset: 0,
            end_of_buffer_offset: 0,
            initial_offset,
            resyncing: initial_offset > 0,
        }
    }

    pub fn last_record_offset(&self) -> u64 {
        self.last_record_offset
    }

    pub fn read_record(&mut self, out: &mut Vec<u8>) -> io::Result<bool> {
        if self.last_record_offset < self.initial_offset && !self.skip_to_initial_block()? {
            return Ok(false);
        }

        out.clear();
        let mut scratch = Vec::<u8>::new();
        let mut in_frag = false;
        let mut prospective_offset = 0u64;

        loop {
            match self.read_physical_record()? {
                Outcome::Eof => {
                    if in_frag {
                        scratch.clear();
                    }
                    return Ok(false);
                }
                Outcome::Bad => {
                    if in_frag {
                        self.report(scratch.len() as u64, "error in middle of record");
                        in_frag = false;
                        scratch.clear();
                    }
                    continue;
                }
                Outcome::Rec {
                    typ,
                    data,
                    physical_offset,
                } => {
                    if self.resyncing {
                        if typ == RecordType::Middle as u8 {
                            continue;
                        }
                        if typ == RecordType::Last as u8 {
                            self.resyncing = false;
                            continue;
                        }
                        self.resyncing = false;
                    }

                    match typ {
                        t if t == RecordType::Full as u8 => {
                            if in_frag && !scratch.is_empty() {
                                self.report(scratch.len() as u64, "partial record without end(1)");
                            }
                            prospective_offset = physical_offset;
                            *out = data;
                            self.last_record_offset = prospective_offset;
                            return Ok(true);
                        }
                        t if t == RecordType::First as u8 => {
                            if in_frag && !scratch.is_empty() {
                                self.report(scratch.len() as u64, "partial record without end(2)");
                            }
                            prospective_offset = physical_offset;
                            scratch = data;
                            in_frag = true;
                        }
                        t if t == RecordType::Middle as u8 => {
                            if !in_frag {
                                self.report(
                                    data.len() as u64,
                                    "missing start of fragmented record(1)",
                                );
                            } else {
                                scratch.extend_from_slice(&data);
                            }
                        }
                        t if t == RecordType::Last as u8 => {
                            if !in_frag {
                                self.report(
                                    data.len() as u64,
                                    "missing start of fragmented record(2)",
                                );
                            } else {
                                scratch.extend_from_slice(&data);
                                *out = std::mem::take(&mut scratch);
                                self.last_record_offset = prospective_offset;
                                return Ok(true);
                            }
                        }
                        other => {
                            let bytes =
                                data.len() as u64 + if in_frag { scratch.len() as u64 } else { 0 };
                            self.report(bytes, &format!("unknown record type {}", other));
                            in_frag = false;
                            scratch.clear();
                        }
                    }
                }
            }
        }
    }

    fn skip_to_initial_block(&mut self) -> io::Result<bool> {
        let off_in_block = (self.initial_offset % BLOCK_SIZE as u64) as usize;
        let mut block_start = self.initial_offset - off_in_block as u64;
        if off_in_block > BLOCK_SIZE - 6 {
            block_start += BLOCK_SIZE as u64;
        }
        self.end_of_buffer_offset = block_start;

        // Move relative by block_start bytes
        if block_start > 0 {
            self.src
                .seek(SeekFrom::Current(block_start as i64))
                .map_err(|e| {
                    self.drop(block_start, &format!("skip failed: {}", e));
                    e
                })?;
        }
        Ok(true)
    }

    fn read_physical_record(&mut self) -> io::Result<Outcome> {
        loop {
            if self.buf.size() < HEADER_SIZE {
                if !self.eof {
                    self.buf.clear();
                    let n = self.src.read(&mut self.backing)?;
                    self.end_of_buffer_offset += n as u64;
                    self.buf.reset_to(&self.backing[..n]);
                    if n < BLOCK_SIZE {
                        self.eof = true;
                    }
                    continue;
                } else {
                    self.buf.clear();
                    return Ok(Outcome::Eof);
                }
            }

            let header = &self.buf.as_slice()[..HEADER_SIZE];
            let a = header[4] as usize;
            let b = header[5] as usize;
            let typ = header[6];
            let len = a | (b << 8);

            if HEADER_SIZE + len > self.buf.size() {
                let drop_sz = self.buf.size() as u64;
                self.buf.clear();
                if !self.eof {
                    self.report(drop_sz, "bad record length");
                    return Ok(Outcome::Bad);
                }
                return Ok(Outcome::Eof);
            }

            if typ == RecordType::Zero as u8 && len == 0 {
                self.buf.clear();
                return Ok(Outcome::Bad);
            }

            let payload_slice = &self.buf.as_slice()[HEADER_SIZE..HEADER_SIZE + len];

            if self.checksum {
                let expected = crc32c::unmask(crc32c::get_fixed32_le(&header[0..4]));
                let actual = crc32c::extend(crc32c::value(&[typ]), payload_slice);
                if actual != expected {
                    let drop_sz = self.buf.size() as u64;
                    self.buf.clear();
                    self.report(drop_sz, "checksum mismatch");
                    return Ok(Outcome::Bad);
                }
            }

            let payload = payload_slice.to_vec();
            self.buf.remove_prefix(HEADER_SIZE + len);
            let remaining = self.buf.size() as u64;

            // Skip physical record that started before initial_offset
            let started_at =
                self.end_of_buffer_offset - remaining - HEADER_SIZE as u64 - len as u64;
            if started_at < self.initial_offset {
                return Ok(Outcome::Bad);
            }

            return Ok(Outcome::Rec {
                typ,
                data: payload,
                physical_offset: started_at,
            });
        }
    }

    fn report(&mut self, bytes: u64, reason: &str) {
        if let Some(rep) = self.reporter.as_mut() {
            if self
                .end_of_buffer_offset
                .saturating_sub(self.buf.size() as u64)
                .saturating_sub(bytes)
                >= self.initial_offset
            {
                rep.corruption(bytes as usize, reason);
            }
        }
    }
    fn drop(&mut self, bytes: u64, reason: &str) {
        self.report(bytes, &format!("drop: {}", reason));
    }

    pub fn into_inner(self) -> (R, Option<Rep>) {
        (self.src, self.reporter)
    }
}

enum Outcome {
    Eof,
    Bad,
    Rec {
        typ: u8,
        data: Vec<u8>,
        physical_offset: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Minimal reporter that records corruption events.
    #[derive(Default)]
    struct CollectingReporter {
        drops: Vec<(usize, String)>,
    }
    impl Reporter for CollectingReporter {
        fn corruption(&mut self, bytes: usize, reason: &str) {
            self.drops.push((bytes, reason.to_string()));
        }
    }

    // Build one physical record: [masked crc32c(type||payload)] [len lo] [len hi] [type] [payload]
    fn phys(typ: u8, payload: &[u8]) -> Vec<u8> {
        // CRC over: type byte + payload, then masked (same as writer).
        let mut tmp = Vec::with_capacity(1 + payload.len());
        tmp.push(typ);
        tmp.extend_from_slice(payload);
        let crc = super::crc32c::value(&tmp);
        let masked = ((crc >> 15) | (crc << 17)).wrapping_add(0xA2_82_EA_D8);

        let mut out = Vec::with_capacity(HEADER_SIZE + payload.len());
        out.extend_from_slice(&masked.to_le_bytes());
        out.push((payload.len() & 0xFF) as u8);
        out.push(((payload.len() >> 8) & 0xFF) as u8);
        out.push(typ);
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn reads_two_full_records() {
        // Build a tiny log with two FULL records.
        let mut log = Vec::new();
        log.extend(phys(1, b"hello"));
        log.extend(phys(1, b"world!"));

        let cursor = Cursor::new(log);
        let rep = CollectingReporter::default();
        let mut r: Reader<_, _> = Reader::new(cursor, Some(rep), true, 0);

        let mut out = Vec::new();
        assert!(r.read_record(&mut out).unwrap());
        assert_eq!(out, b"hello");
        assert!(r.read_record(&mut out).unwrap());
        assert_eq!(out, b"world!");
        assert!(!r.read_record(&mut out).unwrap()); // EOF

        // No corruption expected.
        let (_src, rep_opt) = r.into_inner();
        assert!(rep_opt.unwrap().drops.is_empty());
    }

    #[test]
    fn detects_checksum_mismatch() {
        // One FULL record, then flip a payload byte after the header.
        let mut log = phys(1, b"abcdef");
        let payload_pos = HEADER_SIZE;
        log[payload_pos] ^= 0xFF; // corrupt

        let cursor = Cursor::new(log);
        let rep = CollectingReporter::default();
        let mut r: Reader<_, _> = Reader::new(cursor, Some(rep), true, 0);

        let mut out = Vec::new();
        // Should not return any valid record; should report corruption and hit EOF.
        let ok = r.read_record(&mut out).unwrap();
        assert!(!ok);

        let (_src, rep_opt) = r.into_inner();
        let drops = &rep_opt.unwrap().drops;
        assert!(!drops.is_empty());
        assert!(drops.iter().any(|(_, why)| why.contains("checksum")));
    }

    #[test]
    fn handles_trailer_and_fragmentation_across_blocks() {
        // 1) First FULL record sized to leave a 3-byte trailer => writer would pad zeros.
        // We emulate that: FULL whose header+payload = BLOCK_SIZE - 3, then 3 zeros.
        let pad_len = 3;
        let first_len = BLOCK_SIZE - HEADER_SIZE - pad_len;
        let mut log = Vec::new();
        log.extend(phys(1, &vec![b'x'; first_len]));
        log.extend(std::iter::repeat(0u8).take(pad_len));

        // 2) Now a large logical record that spans two physical records:
        // FIRST payload fills the rest of the block exactly, LAST in next block.
        let first_frag_payload = BLOCK_SIZE - HEADER_SIZE; // fills a block with header
        log.extend(phys(2, &vec![b'y'; first_frag_payload])); // FIRST
        log.extend(phys(4, &vec![b'z'; 10])); // LAST

        let cursor = Cursor::new(log);
        let rep = CollectingReporter::default();
        let mut r: Reader<_, _> = Reader::new(cursor, Some(rep), true, 0);

        let mut out = Vec::new();

        // Record #1: the initial FULL 'x'... payload
        assert!(r.read_record(&mut out).unwrap());
        assert_eq!(out.len(), first_len);
        assert!(out.iter().all(|&b| b == b'x'));

        // Record #2: reassembled large record 'y'... + 'z'...
        assert!(r.read_record(&mut out).unwrap());
        assert_eq!(out.len(), first_frag_payload + 10);
        assert!(out[..first_frag_payload].iter().all(|&b| b == b'y'));
        assert!(out[first_frag_payload..].iter().all(|&b| b == b'z'));

        // EOF
        assert!(!r.read_record(&mut out).unwrap());

        let (_src, rep_opt) = r.into_inner();
        assert!(rep_opt.unwrap().drops.is_empty());
    }

    #[test]
    fn resyncs_when_initial_offset_is_inside_a_fragment() {
        // Construct: [FULL (fills to trailer)] [FIRST .. LAST (big A)] [FULL "B"]
        // Then set initial_offset to inside the FIRST of A, so the reader skips to the next record ("B").
        let pad_len = 3;
        let first_len = BLOCK_SIZE - HEADER_SIZE - pad_len;

        let mut log = Vec::new();
        log.extend(phys(1, &vec![b'x'; first_len]));
        log.extend(std::iter::repeat(0u8).take(pad_len));

        // Big A split into FIRST (fills block) + LAST(10)
        let first_frag_payload = BLOCK_SIZE - HEADER_SIZE;
        let second_start = log.len(); // offset where FIRST of A will begin
        log.extend(phys(2, &vec![b'a'; first_frag_payload])); // FIRST
        log.extend(phys(4, &vec![b'a'; 10])); // LAST

        // Then a small FULL "B"
        log.extend(phys(1, b"B"));

        // Start reading from inside the FIRST-of-A header (simulate initial_offset in the middle)
        let initial_offset = (second_start as u64) + 5;

        let cursor = Cursor::new(log);
        let rep = CollectingReporter::default();
        let mut r: Reader<_, _> = Reader::new(cursor, Some(rep), true, initial_offset);

        let mut out = Vec::new();
        // Should skip the rest of A and return the next logical record: "B"
        assert!(r.read_record(&mut out).unwrap());
        assert_eq!(out, b"B");

        assert!(!r.read_record(&mut out).unwrap());
        let (_src, rep_opt) = r.into_inner();
        // No corruption: resync skips without reporting.
        assert!(rep_opt.unwrap().drops.is_empty());
    }
}
