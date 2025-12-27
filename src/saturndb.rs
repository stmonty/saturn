use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use crate::common::{Entry, Key, Value};
use crate::memtable::MemTable;
use crate::sstable::SSTable;
use crate::wal::WriteAheadLog;

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub memtable_max_entries: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_max_entries: 1000,
        }
    }
}

pub struct SaturnDB {
    dir: PathBuf,
    memtable: Mutex<MemTable>,
    wal: Mutex<WriteAheadLog>,
    sstables: RwLock<Vec<SSTable>>,
    next_sst_id: AtomicU64,
    options: Options,
}

impl SaturnDB {
    pub fn open<P: AsRef<Path>>(dir: P, options: Options) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let wal_path = dir.join("wal.log");
        let wal = WriteAheadLog::new(&wal_path)?;
        let (sstables, next_sst_id) = load_sstables(&dir)?;

        let db = Self {
            dir,
            memtable: Mutex::new(MemTable::with_capacity(options.memtable_max_entries)),
            wal: Mutex::new(wal),
            sstables: RwLock::new(sstables),
            next_sst_id: AtomicU64::new(next_sst_id),
            options,
        };

        db.recover()?;
        Ok(db)
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
            drop(memtable);
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
            drop(memtable);
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
            if sstable.index.contains_key(key) {
                return Ok(None);
            }
        }

        Ok(None)
    }

    pub fn recover(&self) -> std::io::Result<()> {
        let mut iter = {
            let wal = self.wal.lock().unwrap();
            wal.iter()?
        };
        let mut memtable = self.memtable.lock().unwrap();
        while let Some(entry) = iter.next() {
            match entry? {
                Entry::Put { key, value } => memtable.insert(key, value),
                Entry::Delete { key } => memtable.delete(key),
            }
        }
        Ok(())
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.read().unwrap().len()
    }

    pub fn compact_all(&self) -> std::io::Result<()> {
        self.flush_memtable()?;
        let sstables = self.sstables.read().unwrap();
        if sstables.len() <= 1 {
            return Ok(());
        }

        let mut data: BTreeMap<Key, (Value, u64)> = BTreeMap::new();
        let mut tombstones: BTreeMap<Key, u64> = BTreeMap::new();
        let mut seen: BTreeSet<Key> = BTreeSet::new();

        for sstable in sstables.iter().rev() {
            for entry in sstable.iter_entries()? {
                match entry {
                    Entry::Put { key, value } => {
                        if seen.insert(key.clone()) {
                            data.insert(key, (value, 0));
                        }
                    }
                    Entry::Delete { key } => {
                        if seen.insert(key.clone()) {
                            tombstones.insert(key, 0);
                        }
                    }
                }
            }
        }

        let old_paths: Vec<PathBuf> = sstables.iter().map(|sst| sst.file_path.clone()).collect();
        drop(sstables);

        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.dir.join(format!("sst-{}.sst", sst_id));
        let sstable = SSTable::write(data, tombstones, &sstable_path)?;

        {
            let mut sstables = self.sstables.write().unwrap();
            sstables.clear();
            sstables.push(sstable);
        }

        for path in old_paths {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    fn flush_memtable(&self) -> std::io::Result<()> {
        let (data, tombstones) = {
            let mut memtable = self.memtable.lock().unwrap();
            memtable.flush()
        };
        if data.is_empty() && tombstones.is_empty() {
            return Ok(());
        }
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.dir.join(format!("sst-{}.sst", sst_id));
        let sstable = SSTable::write(data, tombstones, &sstable_path)?;
        self.sstables.write().unwrap().push(sstable);
        self.wal.lock().unwrap().reset()?;
        Ok(())
    }
}

fn load_sstables(dir: &Path) -> std::io::Result<(Vec<SSTable>, u64)> {
    let mut found = Vec::new();
    let mut max_id: Option<u64> = None;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = match path.file_name().and_then(|v| v.to_str()) {
            Some(name) => name,
            None => continue,
        };
        if let Some(id) = parse_sst_id(file_name) {
            let table = SSTable::open(&path)?;
            found.push((id, table));
            max_id = Some(max_id.map_or(id, |cur| cur.max(id)));
        }
    }

    found.sort_by_key(|(id, _)| *id);
    let sstables = found.into_iter().map(|(_, table)| table).collect();
    let next_id = max_id.map_or(0, |id| id + 1);
    Ok((sstables, next_id))
}

fn parse_sst_id(file_name: &str) -> Option<u64> {
    let rest = file_name.strip_prefix("sst-")?;
    let rest = rest.strip_suffix(".sst")?;
    rest.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_dir(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nonce: u64 = rand::random();
        dir.push(format!("saturn_{name}_{nonce}"));
        dir
    }

    #[test]
    fn test_sdb_put_get() -> std::io::Result<()> {
        let dir = temp_db_dir("put_get");
        let db = SaturnDB::open(&dir, Options::default())?;

        db.put(b"key1".to_vec(), b"value1".to_vec())?;
        db.put(b"key2".to_vec(), b"value2".to_vec())?;

        let val1 = db.get(&b"key1".to_vec())?;
        let val2 = db.get(&b"key2".to_vec())?;
        let val3 = db.get(&b"key3".to_vec())?;

        assert_eq!(val1, Some(b"value1".to_vec()));
        assert_eq!(val2, Some(b"value2".to_vec()));
        assert_eq!(val3, None);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn test_sdb_delete() -> std::io::Result<()> {
        let dir = temp_db_dir("delete");
        let db = SaturnDB::open(&dir, Options::default())?;

        db.put(b"key1".to_vec(), b"value1".to_vec())?;
        db.delete(b"key1".to_vec())?;

        let val = db.get(&b"key1".to_vec())?;
        assert_eq!(val, None);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn test_sdb_recovery() -> std::io::Result<()> {
        let dir = temp_db_dir("recovery");

        {
            let db = SaturnDB::open(&dir, Options::default())?;
            db.put(b"key1".to_vec(), b"value1".to_vec())?;
            db.put(b"key2".to_vec(), b"value2".to_vec())?;
            db.delete(b"key1".to_vec())?;
        }

        let db = SaturnDB::open(&dir, Options::default())?;

        let val1 = db.get(&b"key1".to_vec())?;
        let val2 = db.get(&b"key2".to_vec())?;

        assert_eq!(val2, Some(b"value2".to_vec()));
        assert_eq!(val1, None);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn test_sdb_compaction_merges_tables() -> std::io::Result<()> {
        let dir = temp_db_dir("compact");
        let options = Options {
            memtable_max_entries: 2,
        };
        let db = SaturnDB::open(&dir, options)?;

        db.put(b"key1".to_vec(), b"value1".to_vec())?;
        db.put(b"key2".to_vec(), b"value2".to_vec())?;
        db.put(b"key1".to_vec(), b"value3".to_vec())?;
        db.put(b"key3".to_vec(), b"value3".to_vec())?;
        db.delete(b"key2".to_vec())?;
        db.put(b"key4".to_vec(), b"value4".to_vec())?;

        assert!(db.sstable_count() >= 2);
        db.compact_all()?;

        assert_eq!(db.sstable_count(), 1);
        assert_eq!(db.get(&b"key1".to_vec())?, Some(b"value3".to_vec()));
        assert_eq!(db.get(&b"key2".to_vec())?, None);
        assert_eq!(db.get(&b"key3".to_vec())?, Some(b"value3".to_vec()));
        assert_eq!(db.get(&b"key4".to_vec())?, Some(b"value4".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
