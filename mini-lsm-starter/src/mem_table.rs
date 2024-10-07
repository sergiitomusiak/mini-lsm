#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

pub(crate) fn map_bound<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    match bound {
        Bound::Included(x) => Bound::Included(f(x)),
        Bound::Excluded(x) => Bound::Excluded(f(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn map_key_slice_to_bytes(key: &KeySlice) -> KeyBytes {
    let key_bytes = Bytes::copy_from_slice(key.key_ref());
    KeyBytes::from_bytes_with_ts(key_bytes, key.ts())
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skipmap = SkipMap::new();
        let wal = Wal::recover(path, &skipmap)?;
        let approximate_size = skipmap
            .iter()
            .map(|item| item.key().raw_len() + item.value().len())
            .sum::<usize>();

        Ok(Self {
            map: Arc::new(skipmap),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(approximate_size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = KeySlice::from_slice(key, TS_DEFAULT);
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        let key = KeySlice::from_slice(key, TS_DEFAULT);
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_bound(lower, |lower| KeySlice::from_slice(lower, TS_DEFAULT)),
            map_bound(upper, |upper| KeySlice::from_slice(upper, TS_DEFAULT)),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.map
            .get(&map_key_slice_to_bytes(&key))
            .map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let approximate_size = data
            .iter()
            .map(|(key, value)| key.raw_len() + value.len())
            .sum();
        self.approximate_size
            .fetch_add(approximate_size, std::sync::atomic::Ordering::AcqRel);

        for (key, value) in data {
            self.map
                .insert(map_key_slice_to_bytes(key), Bytes::copy_from_slice(value));
        }

        if let Some(wal) = self.wal.as_ref() {
            wal.put_batch(data)?;
        }

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut memtable_iter = MemTableIteratorBuilder {
            id: self.id,
            item: None,
            map: self.map.clone(),
            iter_builder: |map| {
                map.range((
                    map_bound(lower, |lower| map_key_slice_to_bytes(&lower)),
                    map_bound(upper, |upper| map_key_slice_to_bytes(&upper)),
                ))
            },
        }
        .build();
        memtable_iter.next().unwrap();
        memtable_iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded);
        while iter.is_valid() {
            builder.add(iter.key(), iter.value());
            iter.next()?;
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    id: usize,
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: Option<(KeyBytes, Bytes)>,
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().as_ref().unwrap().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().as_ref().unwrap().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        self.borrow_item().is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            *fields.item = fields
                .iter
                .next()
                .map(|item| (item.key().clone(), item.value().clone()));
        });
        Ok(())
    }
}
