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
                Outcome::Rec { typ, data } => {
                    // compute physical record start offset
                    let phys_off = self.end_of_buffer_offset
                        - self.buf.size() as u64
                        - HEADER_SIZE as u64
                        - data.len() as u64;

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
                            prospective_offset = phys_off;
                            out.clear();
                            out.extend_from_slice(data);
                            self.last_record_offset = prospective_offset;
                            return Ok(true);
                        }
                        t if t == RecordType::First as u8 => {
                            if in_frag && !scratch.is_empty() {
                                self.report(scratch.len() as u64, "partial record without end(2)");
                            }
                            prospective_offset = phys_off;
                            scratch.clear();
                            scratch.extend_from_slice(data);
                            in_frag = true;
                        }
                        t if t == RecordType::Middle as u8 => {
                            if !in_frag {
                                self.report(
                                    data.len() as u64,
                                    "missing start of fragmented record(1)",
                                );
                            } else {
                                scratch.extend_from_slice(data);
                            }
                        }
                        t if t == RecordType::Last as u8 => {
                            if !in_frag {
                                self.report(
                                    data.len() as u64,
                                    "missing start of fragmented record(2)",
                                );
                            } else {
                                scratch.extend_from_slice(data);
                                out.clear();
                                out.extend_from_slice(&scratch);
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

    fn read_physical_record<'a>(&'a mut self) -> io::Result<Outcome<'a>> {
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

            if self.checksum {
                let expected = crc32c::unmask(get_fixed32_le(&header[0..4]));
                let actual = crc32c::value(&header[6..6 + 1 + len]);
                if actual != expected {
                    let drop_sz = self.buf.size() as u64;
                    self.buf.clear();
                    self.report(drop_sz, "checksum mismatch");
                    return Ok(Outcome::Bad);
                }
            }

            let payload = &self.buf.as_slice()[HEADER_SIZE..HEADER_SIZE + len];
            self.buf.remove_prefix(HEADER_SIZE + len);

            // Skip physical record that started before initial_offset
            let started_at = self.end_of_buffer_offset
                - self.buf.size() as u64
                - HEADER_SIZE as u64
                - len as u64;
            if started_at < self.initial_offset {
                return Ok(Outcome::Bad);
            }

            return Ok(Outcome::Rec { typ, data: payload });
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

enum Outcome<'a> {
    Eof,
    Bad,
    Rec { typ: u8, data: &'a [u8] },
}
