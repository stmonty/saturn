// +------------------+
// |   Key-Value      |
// |     Store        |
// +--------+---------+
//          |
//          | PUT "name" -> "Alice"
//          | PUT "age" -> "30"
//          | DELETE "age"
//          |
//          v
// +------------------+
// |    MemTable      |
// | Sorted Map:      |
// | "name" -> "Alice"|
// | Tombstone: "age" |
// +--------+---------+
//          |
//          | MemTable Full (e.g., size limit reached)
//          |
//          v
// +------------------+         +------------------+
// |     SSTable 1    |         |     SSTable 2    |
// | Sorted Keys:     |         | Sorted Keys:     |
// | "name" -> "Alice"|         | "age" -> Tombstone|
// +------------------+         +------------------+

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::bloom_filter::BloomFilter;
use crate::common::{Entry, Key, SequenceNumber, Value};
use crate::wal::read_bytes;

pub struct SSTable {
    pub file_path: PathBuf,
    pub index: BTreeMap<Key, u64>, // Key to file offset
    bloom_filter: BloomFilter,
}

impl SSTable {
    pub fn new<P: AsRef<Path>>(
        file_path: P,
        index: BTreeMap<Key, u64>,
        bloom_filter: BloomFilter,
    ) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            index,
            bloom_filter,
        }
    }

    pub fn write<P>(
        data: BTreeMap<Key, (Value, SequenceNumber)>,
        tombstones: BTreeMap<Key, SequenceNumber>,
        file_path: &P,
    ) -> std::io::Result<SSTable>
    where
        P: AsRef<Path> + ?Sized,
    {
        let mut file = BufWriter::new(File::create(file_path)?);
        let mut index = BTreeMap::new();
        let mut bloom_filter = BloomFilter::default();

        for (key, (value, sequence_number)) in data {
            let offset = file.seek(SeekFrom::Current(0))?;
            // Serialize entry
            write_entry(&mut file, 0, &key, Some(&value), sequence_number)?;
            index.insert(key.clone(), offset);
            bloom_filter.add(&key);
        }

        for (key, sequence_number) in tombstones {
            let offset = file.seek(SeekFrom::Current(0))?;
            write_entry(&mut file, 1, &key, None, sequence_number)?;
            index.insert(key.clone(), offset);
            bloom_filter.add(&key);
        }

        Ok(SSTable::new(file_path, index, bloom_filter))
    }

    pub fn get(&self, key: &Key) -> std::io::Result<Option<(Value, SequenceNumber)>> {
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }

        if let Some(&offset) = self.index.get(key) {
            let mut file = File::open(&self.file_path)?;
            file.seek(SeekFrom::Start(offset))?;
            read_entry(&mut file).map(|entry| match entry {
                Entry::Put { key: _, value } => Some((value, 0)), // Sequence number can be stored if needed
                Entry::Delete { .. } => None,
            })
        } else {
            Ok(None)
        }
    }
}

pub fn write_entry<W: Write>(
    writer: &mut W,
    entry_type: u8,
    key: &Key,
    value: Option<&Value>,
    sequence_number: SequenceNumber,
) -> std::io::Result<()> {
    writer.write_all(&[entry_type])?;
    writer.write_all(&(sequence_number.to_be_bytes()))?;
    writer.write_all(&(key.len() as u32).to_be_bytes())?;
    writer.write_all(key)?;
    if let Some(value) = value {
        writer.write_all(&(value.len() as u32).to_be_bytes())?;
        writer.write_all(value)?;
    }
    Ok(())
}

pub fn read_entry<R: Read>(reader: &mut R) -> std::io::Result<Entry> {
    let mut type_byte = [0u8; 1];
    reader.read_exact(&mut type_byte)?;
    let mut seq_bytes = [0u8; 8];
    reader.read_exact(&mut seq_bytes)?;
    //let sequence_number = u64::from_be_bytes(seq_bytes);
    let key = read_bytes(reader).ok_or(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Invalid key",
    ))?;
    match type_byte[0] {
        0 => {
            let value = read_bytes(reader).ok_or(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid value",
            ))?;
            Ok(Entry::Put { key, value })
        }
        1 => Ok(Entry::Delete { key }),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid entry type",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_sstable_write_and_get() -> std::io::Result<()> {
        let file_path = Path::new("/tmp/sstable.db");

        let mut data = BTreeMap::new();
        data.insert(b"key1".to_vec(), (b"value1".to_vec(), 1));
        data.insert(b"key2".to_vec(), (b"value2".to_vec(), 2));

        let mut tombstones = BTreeMap::new();
        tombstones.insert(b"key3".to_vec(), 3);

        let sstable = SSTable::write(data.clone(), tombstones.clone(), file_path)?;
        // Test existing keys
        let retrieved = sstable.get(&b"key1".to_vec())?;
        assert_eq!(retrieved, Some((b"value1".to_vec(), 0)));

        let retrieved = sstable.get(&b"key2".to_vec())?;
        assert_eq!(retrieved, Some((b"value2".to_vec(), 0)));

        // Test tombstone key
        let retrieved = sstable.get(&b"key3".to_vec())?;
        assert_eq!(retrieved, None);

        // Test non-existing key
        let retrieved = sstable.get(&b"key4".to_vec())?;
        assert_eq!(retrieved, None);
        Ok(())
    }
}
