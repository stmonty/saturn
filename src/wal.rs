use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use crate::common::Entry;
use crate::wal_reader::{Reader, Reporter};
use crate::wal_writer::Writer;

const PUT_TAG: u8 = 0;
const DELETE_TAG: u8 = 1;

pub struct WriteAheadLog {
    path: PathBuf,
    writer: Writer<File>,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path_buf)?;
        let len = file.metadata()?.len();
        let writer = Writer::with_starting_offset(file, len);
        Ok(Self {
            path: path_buf,
            writer,
        })
    }

    pub fn append(&mut self, entry: &Entry) -> io::Result<()> {
        let mut payload = Vec::new();
        encode_entry(entry, &mut payload);
        self.writer.add_record(&payload)
    }

    pub fn iter(&self) -> io::Result<WriteAheadLogIter> {
        WriteAheadLogIter::new(&self.path)
    }

    pub fn into_inner(self) -> File {
        self.writer.into_inner()
    }
}

pub struct WriteAheadLogIter {
    reader: Reader<BufReader<File>, NoopReporter>,
    record: Vec<u8>,
}

impl WriteAheadLogIter {
    fn new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = Reader::new(BufReader::new(file), Some(NoopReporter::default()), true, 0);
        Ok(Self {
            reader,
            record: Vec::new(),
        })
    }
}

impl Iterator for WriteAheadLogIter {
    type Item = io::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_record(&mut self.record) {
                Ok(true) => match decode_entry(&self.record) {
                    Ok(entry) => return Some(Ok(entry)),
                    Err(err) => return Some(Err(err)),
                },
                Ok(false) => return None,
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

#[derive(Default)]
struct NoopReporter;

impl Reporter for NoopReporter {
    fn corruption(&mut self, _bytes: usize, _reason: &str) {}
}

fn encode_entry(entry: &Entry, dst: &mut Vec<u8>) {
    match entry {
        Entry::Put { key, value } => {
            dst.push(PUT_TAG);
            write_len_prefixed(key, dst);
            write_len_prefixed(value, dst);
        }
        Entry::Delete { key } => {
            dst.push(DELETE_TAG);
            write_len_prefixed(key, dst);
        }
    }
}

fn decode_entry(src: &[u8]) -> io::Result<Entry> {
    if src.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty WAL record",
        ));
    }
    let tag = src[0];
    let mut offset = 1;
    let key = read_len_prefixed(src, &mut offset)?;
    match tag {
        PUT_TAG => {
            let value = read_len_prefixed(src, &mut offset)?;
            Ok(Entry::Put { key, value })
        }
        DELETE_TAG => Ok(Entry::Delete { key }),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown WAL tag {}", tag),
        )),
    }
}

fn write_len_prefixed(bytes: &[u8], dst: &mut Vec<u8>) {
    let len = bytes.len() as u32;
    dst.extend(len.to_be_bytes());
    dst.extend_from_slice(bytes);
}

fn read_len_prefixed(src: &[u8], offset: &mut usize) -> io::Result<Vec<u8>> {
    if src.len().saturating_sub(*offset) < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "missing length",
        ));
    }
    let len = u32::from_be_bytes([
        src[*offset],
        src[*offset + 1],
        src[*offset + 2],
        src[*offset + 3],
    ]) as usize;
    *offset += 4;
    if src.len().saturating_sub(*offset) < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "payload truncated",
        ));
    }
    let out = src[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(out)
}

pub fn read_bytes<R: Read>(reader: &mut R) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    if reader.read_exact(&mut len_buf).is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    if reader.read_exact(&mut buf).is_err() {
        return None;
    }
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Entry;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("saturn_wal_{name}.log"));
        path
    }

    #[test]
    fn append_and_iterate_round_trip() {
        let path = temp_path("round_trip");
        let _ = fs::remove_file(&path);

        let mut wal = WriteAheadLog::new(&path).unwrap();
        wal.append(&Entry::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        })
        .unwrap();
        wal.append(&Entry::Delete {
            key: b"k2".to_vec(),
        })
        .unwrap();
        wal.append(&Entry::Put {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
        })
        .unwrap();

        let mut entries = wal.iter().unwrap();

        assert_eq!(
            entries.next().unwrap().unwrap(),
            Entry::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }
        );
        assert_eq!(
            entries.next().unwrap().unwrap(),
            Entry::Delete {
                key: b"k2".to_vec(),
            }
        );
        assert_eq!(
            entries.next().unwrap().unwrap(),
            Entry::Put {
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
            }
        );
        assert!(entries.next().is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn iterator_handles_empty_log() {
        let path = temp_path("empty");
        let _ = fs::remove_file(&path);
        let wal = WriteAheadLog::new(&path).unwrap();
        let mut iter = wal.iter().unwrap();
        assert!(iter.next().is_none());
        let _ = fs::remove_file(&path);
    }
}
