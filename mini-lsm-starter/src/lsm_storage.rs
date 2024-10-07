#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::{
    merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
};
use crate::key::{self, Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::txn::TxnIterator;
use crate::mvcc::{txn::Transaction, LsmMvccInner};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        {
            self.compaction_notifier.send(()).ok();
            if let Some(compaction_thread) = self.compaction_thread.lock().take() {
                compaction_thread.join().map_err(|e| anyhow!("{e:?}"))?;
            }
        }

        {
            self.flush_notifier.send(()).ok();
            if let Some(flush_thread) = self.flush_thread.lock().take() {
                flush_thread.join().map_err(|e| anyhow!("{e:?}"))?;
            }
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // Flush current memtable
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(MemTable::create(self.inner.next_sst_id()))?;
        }

        // Flush all immutable memtables
        while {
            let state = self.inner.state.read();
            !state.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<TxnIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20));
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let mut next_sst_id = 1;
        let manifest_path = path.join("MANIFEST");
        let (manifest, last_commit_ts) = if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            let manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            (manifest, 0)
        } else {
            let (manifest, records) = Manifest::recover(manifest_path)?;
            let mut memtables = BTreeSet::new();
            let mut last_commit_ts = 0;
            for record in records {
                match record {
                    ManifestRecord::Flush(sstable_id) => {
                        let removed = memtables.remove(&sstable_id);
                        assert!(removed, "memtable is missing");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sstable_id);
                        } else {
                            state.levels.insert(0, (sstable_id, vec![sstable_id]));
                        }
                        next_sst_id = next_sst_id.max(sstable_id);
                    }
                    ManifestRecord::Compaction(task, sstable_ids) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            &sstable_ids,
                            true,
                        );

                        state = new_state;
                        next_sst_id = next_sst_id.max(
                            sstable_ids
                                .iter()
                                .max()
                                .copied()
                                .expect("at least one sstable must be present"),
                        );
                    }
                    ManifestRecord::NewMemtable(memtable_id) => {
                        next_sst_id = next_sst_id.max(memtable_id);
                        memtables.insert(memtable_id);
                    }
                }
            }

            let sstable_ids = state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files));
            for sstable_id in sstable_ids {
                let sstable_id = *sstable_id;
                let sstable = SsTable::open(
                    sstable_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sstable_id))?,
                )?;
                last_commit_ts = last_commit_ts.max(sstable.max_ts());
                state.sstables.insert(sstable_id, Arc::new(sstable));
            }

            if matches!(compaction_controller, CompactionController::Leveled(_)) {
                for (_, sstable_ids) in &mut state.levels {
                    sstable_ids.sort_by(|sstable_1, sstable_2| {
                        state
                            .sstables
                            .get(sstable_1)
                            .expect("sstable must exist")
                            .first_key()
                            .cmp(
                                state
                                    .sstables
                                    .get(sstable_2)
                                    .expect("sstable must exist")
                                    .first_key(),
                            )
                    });
                }
            }

            next_sst_id += 1;
            // recover memtables
            state.memtable = if options.enable_wal {
                for sstable_id in memtables {
                    let memtable = MemTable::recover_from_wal(
                        sstable_id,
                        Self::path_of_wal_static(path, sstable_id),
                    )?;

                    let max_ts = memtable
                        .map
                        .iter()
                        .map(|item| item.key().ts())
                        .max()
                        .unwrap_or_default();

                    last_commit_ts = last_commit_ts.max(max_ts);

                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?)
            } else {
                Arc::new(MemTable::create(next_sst_id))
            };

            manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;
            next_sst_id += 1;
            (manifest, last_commit_ts)
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(last_commit_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(key)
    }

    pub(crate) fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let iter = self.scan_with_ts(Bound::Included(key), Bound::Included(key), read_ts)?;

        if iter.is_valid() && iter.key() == key {
            let value = Some(iter.value())
                .filter(|value| !value.is_empty())
                .map(Bytes::copy_from_slice);

            return Ok(value);
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _write_lock = self.mvcc().write_lock.lock();
        let last_commit_ts = self.mvcc().latest_commit_ts() + 1;

        let batch = batch
            .iter()
            .map(|record| {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        let key = key.as_ref();
                        let value = value.as_ref();
                        assert!(!key.is_empty(), "key cannot be empty");
                        assert!(!value.is_empty(), "value cannot be empty");
                        // self.put_internal(key, value, last_commit_ts)?;
                        (KeySlice::from_slice(key, last_commit_ts), value)
                    }
                    WriteBatchRecord::Del(key) => {
                        let key = key.as_ref();
                        assert!(!key.is_empty(), "key cannot be empty");
                        //self.put_internal(key, b"", last_commit_ts)?;
                        (KeySlice::from_slice(key, last_commit_ts), b"".as_ref())
                    }
                }
            })
            .collect::<Vec<_>>();

        self.write_batch_internal(&batch)?;

        // for record in batch {
        //     match record {
        //         WriteBatchRecord::Put(key, value) => {
        //             let key = key.as_ref();
        //             let value = value.as_ref();
        //             assert!(!key.is_empty(), "key cannot be empty");
        //             assert!(!value.is_empty(), "value cannot be empty");
        //             self.put_internal(key, value, last_commit_ts)?;
        //         }
        //         WriteBatchRecord::Del(key) => {
        //             let key = key.as_ref();
        //             assert!(!key.is_empty(), "key cannot be empty");
        //             self.put_internal(key, b"", last_commit_ts)?;
        //         }
        //     }
        // }

        self.mvcc().update_commit_ts(last_commit_ts);
        Ok(())
    }

    pub fn write_batch_internal(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        {
            let state = self.state.read();
            state.memtable.put_batch(data)?;
        }

        if self.memtable_reached_capacity() {
            let state_lock = self.state_lock.lock();
            if self.memtable_reached_capacity() {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    pub fn put_internal(&self, key: &[u8], value: &[u8], last_commit_ts: u64) -> Result<()> {
        {
            let state = self.state.read();
            state
                .memtable
                .put(Key::from_slice(key, last_commit_ts), value)?;
        }

        if self.memtable_reached_capacity() {
            let state_lock = self.state_lock.lock();
            if self.memtable_reached_capacity() {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        std::fs::File::open(self.path.clone())
            .context("open storage directory")?
            .sync_all()
            .context("sync storage directory")
    }

    fn freeze_memtable_with_memtable(&self, memtable: MemTable) -> Result<()> {
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let imm_memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(memtable));
        snapshot.imm_memtables.insert(0, imm_memtable.clone());
        *guard = Arc::new(snapshot);
        drop(guard);
        imm_memtable.sync_wal()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            MemTable::create_with_wal(memtable_id, self.path_of_wal(memtable_id))?
        } else {
            MemTable::create(memtable_id)
        };

        self.freeze_memtable_with_memtable(memtable)?;

        self.manifest
            .as_ref()
            .expect("manifest must exist")
            .add_record(
                state_lock_observer,
                ManifestRecord::NewMemtable(memtable_id),
            )?;

        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let memtable_to_flush = {
            let state = self.state.read();
            let Some(memtable_to_flush) = state.imm_memtables.last() else {
                return Ok(());
            };

            Arc::clone(memtable_to_flush)
        };

        let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sstable_builder)?;

        let sstable_id = memtable_to_flush.id();
        let sstable_path = self.path_of_sst(sstable_id);
        let sstable = sstable_builder.build(
            sstable_id,
            Some(Arc::clone(&self.block_cache)),
            sstable_path,
        )?;

        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sstable_id);
            } else {
                snapshot.levels.insert(0, (sstable_id, vec![sstable_id]));
            }

            snapshot.sstables.insert(sstable_id, Arc::new(sstable));
            snapshot.imm_memtables.pop();

            self.sync_dir()?;

            self.manifest
                .as_ref()
                .expect("manifest must exist")
                .add_record(&state_lock, ManifestRecord::Flush(sstable_id))?;

            *state = Arc::new(snapshot);
        }

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        Ok(txn)
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        self: &Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<TxnIterator>> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper).map(FusedIterator::new)
    }

    pub(crate) fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        let mut memtable_iters = vec![Box::new(state.memtable.scan(
            map_bound(lower, |lower| Key::from_slice(lower, key::TS_RANGE_BEGIN)),
            map_bound(upper, |upper| Key::from_slice(upper, key::TS_RANGE_END)),
        ))];

        for imm_memtable in state.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_memtable.scan(
                map_bound(lower, |lower| Key::from_slice(lower, key::TS_RANGE_BEGIN)),
                map_bound(upper, |upper| Key::from_slice(upper, key::TS_RANGE_END)),
            )));
        }

        let mut l0_sstable_iters = Vec::with_capacity(state.l0_sstables.len());
        for l0_sstable_id in state.l0_sstables.iter() {
            let Some(l0_sstable) = state.sstables.get(l0_sstable_id) else {
                bail!("sstable with id={l0_sstable_id} does not exist");
            };

            let does_range_overlap = range_overlap(
                l0_sstable.first_key().key_ref(),
                l0_sstable.last_key().key_ref(),
                lower,
                upper,
            );

            if !does_range_overlap {
                continue;
            }

            let iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                    Arc::clone(l0_sstable),
                    Key::from_slice(key, key::TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(l0_sstable),
                        Key::from_slice(key, key::TS_RANGE_BEGIN),
                    )?;
                    if iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => {
                    SsTableIterator::create_and_seek_to_first(Arc::clone(l0_sstable))?
                }
            };

            l0_sstable_iters.push(Box::new(iter));
        }

        let mut level_iters = Vec::new();
        for level in 0..state.levels.len() {
            let mut sstables = Vec::new();
            let mut matched_once = false;
            for sstable_id in state.levels[level].1.iter() {
                let Some(sstable) = state.sstables.get(sstable_id) else {
                    bail!("sstable with id={sstable_id} is missing");
                };

                let does_range_overlap = range_overlap(
                    sstable.first_key().key_ref(),
                    sstable.last_key().key_ref(),
                    lower,
                    upper,
                );

                if !does_range_overlap {
                    // TODO: Add optimization: if key range of current SSTable
                    // is already passed input key range, then break current loop to
                    // stop checking other SSTables.
                    if matched_once {
                        // Current SSTable is already passed
                        // current key range. Thus current and further
                        // SSTables do not contain queried keys and can be ignored.
                        break;
                    } else {
                        // At this point none of the SSTables contained requested key range.
                        // Thus continue checking other tables.
                        continue;
                    }
                }

                matched_once = true;
                sstables.push(Arc::clone(sstable));
            }

            let level_iter = match lower {
                Bound::Included(lower) => SstConcatIterator::create_and_seek_to_key(
                    sstables,
                    Key::from_slice(lower, key::TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(lower) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        sstables,
                        Key::from_slice(lower, key::TS_RANGE_BEGIN),
                    )?;
                    if iter.is_valid() && iter.key().key_ref() == lower {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables)?,
            };

            level_iters.push(Box::new(level_iter));
        }

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(
                    MergeIterator::create(memtable_iters),
                    MergeIterator::create(l0_sstable_iters),
                )?,
                MergeIterator::create(level_iters),
            )?,
            map_bound(upper, Bytes::copy_from_slice),
            read_ts,
        )?))
    }

    fn memtable_reached_capacity(&self) -> bool {
        let state = self.state.read();
        state.memtable.approximate_size() >= self.options.target_sst_size
    }
}

pub fn key_within(key: &[u8], lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    let above_lower = match lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };

    let below_upper = match upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };

    above_lower && below_upper
}

pub fn key_less_or_equal(key: &[u8], lower: Bound<&[u8]>) -> bool {
    match lower {
        Bound::Included(lower) => key <= lower,
        Bound::Excluded(lower) => key < lower,
        Bound::Unbounded => false,
    }
}

pub fn key_greater_or_equal(key: &[u8], upper: Bound<&[u8]>) -> bool {
    match upper {
        Bound::Included(upper) => key >= upper,
        Bound::Excluded(upper) => key > upper,
        Bound::Unbounded => false,
    }
}

fn range_overlap(
    lower: &[u8],
    upper: &[u8],
    lower_bound: Bound<&[u8]>,
    upper_bound: Bound<&[u8]>,
) -> bool {
    key_within(lower, lower_bound, upper_bound)
        || key_within(upper, lower_bound, upper_bound)
        || (key_less_or_equal(lower, lower_bound) && key_greater_or_equal(upper, upper_bound))
}
