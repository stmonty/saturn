use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::common::Entry;

pub struct WriteAheadLog {
    writer: BufWriter<std::fs::File>,
    reader: BufReader<std::fs::File>,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let writer_file = OpenOptions::new().create(true).append(true).open(&path)?;
        let reader_file = OpenOptions::new().read(true).open(&path)?;
        Ok(Self {
            writer: BufWriter::new(writer_file),
            reader: BufReader::new(reader_file),
        })
    }

    pub fn append(&mut self, entry: &Entry) -> std::io::Result<()> {
        // Serialize entry
        let serialized = match entry {
            Entry::Put { key, value } => {
                let mut buf = vec![0u8]; // 0 indicates Put
                buf.extend(&(key.len() as u32).to_be_bytes());
                buf.extend(key);
                buf.extend(&(value.len() as u32).to_be_bytes());
                buf.extend(value);
                buf
            }
            Entry::Delete { key } => {
                let mut buf = vec![1u8]; // 1 indicates Delete
                buf.extend(&(key.len() as u32).to_be_bytes());
                buf.extend(key);
                buf
            }
        };
        self.writer.write_all(&serialized)?;
        self.writer.flush()
    }

    pub fn iter(&mut self) -> WALIterator {
        WALIterator {
            reader: &mut self.reader,
        }
    }
}

pub struct WALIterator<'a> {
    reader: &'a mut BufReader<std::fs::File>,
}

impl<'a> Iterator for WALIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut type_byte = [0u8; 1];
        if self.reader.read_exact(&mut type_byte).is_err() {
            return None;
        }
        match type_byte[0] {
            0 => {
                // Put entry
                let key = read_bytes(self.reader)?;
                let value = read_bytes(self.reader)?;
                Some(Entry::Put { key, value })
            }
            1 => {
                // Delete entry
                let key = read_bytes(self.reader)?;
                Some(Entry::Delete { key })
            }
            _ => None,
        }
    }
}

pub fn read_bytes<R: Read>(reader: &mut R) -> Option<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    if reader.read_exact(&mut len_bytes).is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    if reader.read_exact(&mut buf).is_err() {
        return None;
    }
    Some(buf)
}
