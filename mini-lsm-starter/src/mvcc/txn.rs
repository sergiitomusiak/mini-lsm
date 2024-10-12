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

use crate::key::KeyBytes;
use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
    mem_table::map_bound,
    mvcc::CommittedTxnData,
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
        self.record_read(key);
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
        let mut iter = TxnIterator::create(self.clone(), iter)?;
        iter.record_current_key();
        Ok(iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_commited()?;
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut key_hashes = key_hashes.lock();
            key_hashes.1.insert(farmhash::hash32(key));
        }
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, b"")
    }

    pub fn commit(&self) -> Result<()> {
        self.check_commited()?;
        self.committed.store(true, Ordering::Release);
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let last_commit_ts = self.inner.mvcc().latest_commit_ts() + 1;

        let committed_txn_data = if let Some(key_hashes) = self.key_hashes.as_ref() {
            // Txn is serializable. Validatation is request.
            let mut key_hashes = key_hashes.lock();
            dbg!(format!(
                "Committing Txn(read_ts={:?}, commit_ts={:?}, read_write={:?}",
                self.read_ts, last_commit_ts, key_hashes
            ));
            if !key_hashes.1.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();

                let committed_keys = committed_txns
                    .range((
                        Bound::Excluded(self.read_ts),
                        Bound::Excluded(last_commit_ts),
                    ))
                    .flat_map(|txn| txn.1.key_hashes.iter());

                for committed_key in committed_keys {
                    if key_hashes.0.contains(committed_key) {
                        return Err(anyhow!("Aborting transaction: There is a conflict with one of the previous committed transactions"));
                    }
                }

                Some(CommittedTxnData {
                    key_hashes: std::mem::take(&mut key_hashes.1),
                    read_ts: self.read_ts,
                    commit_ts: last_commit_ts,
                })
            } else {
                None
            }
        } else {
            None
        };

        let write_batch = self
            .local_storage
            .iter()
            .map(|entry| {
                (
                    KeyBytes::from_bytes_with_ts(entry.key().clone(), last_commit_ts),
                    entry.value().clone(),
                )
            })
            .collect::<Vec<_>>();

        let write_batch = write_batch
            .iter()
            .map(|(key, value)| (key.as_key_slice(), value.as_ref()))
            .collect::<Vec<_>>();

        self.inner.write_batch_internal(&write_batch)?;

        if let Some(committed_txn_data) = committed_txn_data {
            self.inner
                .mvcc()
                .committed_txns
                .lock()
                .insert(last_commit_ts, committed_txn_data);
        }

        {
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();

            let watermark = self.inner.mvcc().watermark();
            while committed_txns
                .first_key_value()
                .as_ref()
                .map(|(key, _)| **key < watermark)
                .unwrap_or(false)
            {
                let removed = committed_txns.pop_first();
                assert!(removed.is_some());
            }
        }

        self.inner.mvcc().update_commit_ts(last_commit_ts);
        Ok(())
    }

    pub fn check_commited(&self) -> Result<()> {
        if self.committed.as_ref().load(Ordering::Acquire) {
            return Err(anyhow!("transaction is already commited"));
        }
        Ok(())
    }

    fn record_read(self: &Arc<Self>, key: &[u8]) {
        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let key_hash = farmhash::hash32(key);
            let mut key_hashes = key_hashes.lock();
            key_hashes.0.insert(key_hash);
        }
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

    fn record_current_key(&mut self) {
        if self.is_valid() {
            let key = self.iter.key();
            let key_hash = farmhash::hash32(key);
            self.txn.record_read(key);
        }
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
            self.record_current_key();
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
