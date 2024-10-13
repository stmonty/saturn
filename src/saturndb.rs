use std::sync::{Arc, Mutex, RwLock};

use crate::memtable::MemTable;
use crate::sstable::SSTable;
use crate::wal::WriteAheadLog;
use crate::common::{Key, Value, Entry};

pub struct SaturnDB {
    pub memtable: Arc<Mutex<MemTable>>,
    pub wal: Arc<Mutex<WriteAheadLog>>,
    pub sstables: Arc<RwLock<Vec<SSTable>>>,
}

impl SaturnDB {
    pub fn new(wal_path: &str) -> std::io::Result<Self> {
        Ok(Self {
            memtable: Arc::new(Mutex::new(MemTable::new())),
            wal: Arc::new(Mutex::new(WriteAheadLog::new(wal_path)?)),
            sstables: Arc::new(RwLock::new(Vec::new())),
        })
    }

    pub fn put(&self, key: Key, value: Value) -> std::io::Result<()> {
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(&Entry::Put {
                key: key.clone(),
                value: value.clone(),
            })?;
        }

        let mut memtable = self.memtable.lock().unwrap();
        memtable.insert(key, value);

        if memtable.is_full() {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn delete(&self, key: Key) -> std::io::Result<()> {
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(&Entry::Delete { key: key.clone() })?;
        }

        let mut memtable = self.memtable.lock().unwrap();
        memtable.delete(key);

        if memtable.is_full() {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &Key) -> std::io::Result<Option<Value>> {
        {
            let memtable = self.memtable.lock().unwrap();
            if let Some((value, _)) = memtable.get(key) {
                return Ok(Some(value.clone()));
            }
            if memtable.tombstones.contains_key(key) {
                return Ok(None);
            }
        }

        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter().rev() {
            if let Some((value, _)) = sstable.get(key)? {
                return Ok(Some(value));
            }
            // Check for tombstone
            if sstable.index.contains_key(key) {
                // Since we read the index, we need to check if it's a tombstone
                if let Some(entry) = sstable.get(key)? {
                    return Ok(Some(entry.0));
                } else {
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    fn flush_memtable(&self) -> std::io::Result<()> {
        let (data, tombstones) = {
            let mut memtable = self.memtable.lock().unwrap();
            memtable.flush()
        };
        let sstable_path = format!("sstable_{}.db", self.sstables.read().unwrap().len());
        let sstable = SSTable::write(data, tombstones, &sstable_path)?;
        self.sstables.write().unwrap().push(sstable);
        Ok(())
    }

    pub fn recover(&self, wal_path: &str) -> std::io::Result<()> {
        let mut wal = WriteAheadLog::new(wal_path)?;
        let mut memtable = self.memtable.lock().unwrap();
        for entry in wal.iter() {
            match entry {
                Entry::Put { key, value } => {
                    memtable.insert(key, value);
                }
                Entry::Delete { key } => {
                    memtable.delete(key);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sdb_put_get() -> std::io::Result<()> {
        let path = "/tmp/test_sdb_put_get";
        let db = SaturnDB::new(path)?;

        db.put(b"key1".to_vec(), b"value1".to_vec())?;
        db.put(b"key2".to_vec(), b"value2".to_vec())?;

        let val1 = db.get(&b"key1".to_vec())?;
        let val2 = db.get(&b"key2".to_vec())?;
        let val3 = db.get(&b"key3".to_vec())?;

        assert_eq!(val1, Some(b"value1".to_vec()));
        assert_eq!(val2, Some(b"value2".to_vec()));
        assert_eq!(val3, None);
        Ok(())
    }

    #[test]
    fn test_sdb_delete() -> std::io::Result<()> {
        let path = "/tmp/test_sdb_delete";
        let db = SaturnDB::new(path)?;

        db.put(b"key1".to_vec(), b"value1".to_vec())?;
        db.delete(b"key1".to_vec())?;

        let val = db.get(&b"key1".to_vec())?;
        assert_eq!(val, None);
        Ok(())
    }

    #[test]
    fn test_sdb_recovery() -> std::io::Result<()> {
        let path = "/tmp/test_sdb_recovery";

        {
            let db = SaturnDB::new(path)?;
            db.put(b"key1".to_vec(), b"value1".to_vec())?;
            db.put(b"key2".to_vec(), b"value2".to_vec())?;
            db.delete(b"key1".to_vec())?;
        }

        let db = SaturnDB::new(path)?;
        db.recover(path)?;

        let val1 = db.get(&b"key1".to_vec())?;
        let val2 = db.get(&b"key2".to_vec())?;

        assert_eq!(val2, Some(b"value2".to_vec()));
        assert_eq!(val1, None);
        Ok(())
    }
}
