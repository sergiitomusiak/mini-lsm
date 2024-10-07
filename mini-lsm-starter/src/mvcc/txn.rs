use anyhow::anyhow;
use std::sync::atomic::Ordering;

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::lsm_storage::WriteBatchRecord;
use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
    mem_table::map_bound,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_commited()?;
        let iter = self.scan(Bound::Included(key), Bound::Included(key))?;

        if iter.is_valid() && iter.key() == key {
            let value = Some(iter.value())
                .filter(|value| !value.is_empty())
                .map(Bytes::copy_from_slice);

            return Ok(value);
        }

        Ok(None)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.check_commited()?;
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            item: None,
            iter_builder: |map| {
                map.range((
                    map_bound(lower, Bytes::copy_from_slice),
                    map_bound(upper, Bytes::copy_from_slice),
                ))
            },
        }
        .build();
        local_iter.next()?;
        let storage_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let iter = TwoMergeIterator::create(local_iter, storage_iter)?;
        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_commited()?;
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.check_commited()?;
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.check_commited()?;
        self.committed.store(true, Ordering::Release);
        let write_batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if !entry.value().is_empty() {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                } else {
                    WriteBatchRecord::Del(entry.key().clone())
                }
            })
            .collect::<Vec<_>>();
        self.inner.write_batch(&write_batch)?;
        Ok(())
    }

    pub fn check_commited(&self) -> Result<()> {
        if self.committed.as_ref().load(Ordering::Acquire) {
            return Err(anyhow!("transaction is already commited"));
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts = self.inner.mvcc().ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: Option<(Bytes, Bytes)>,
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().as_ref().unwrap().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().as_ref().unwrap().0.as_ref()
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

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        loop {
            self.iter.next()?;
            if !self.is_valid() || !self.value().is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
