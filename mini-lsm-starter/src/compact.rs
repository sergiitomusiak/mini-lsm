#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        match task {
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                assert!(!tiers.is_empty());

                let mut tier_iters = Vec::new();
                for (i, tier) in tiers.iter().enumerate() {
                    assert_eq!(tier.0, snapshot.levels[i].0);
                    let mut sstables = Vec::new();
                    for sstable_id in tier.1.iter() {
                        let Some(sstable) = snapshot.sstables.get(sstable_id) else {
                            bail!("sstable with id={sstable_id} is missing");
                        };
                        sstables.push(sstable.clone());
                    }
                    tier_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        sstables,
                    )?));
                }

                let iter = MergeIterator::create(tier_iters);
                self.compact_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let mut upper_level_sstables = Vec::new();
                for sstable_id in upper_level_sst_ids {
                    let Some(sstable) = snapshot.sstables.get(sstable_id) else {
                        bail!("sstable with id={sstable_id} is missing");
                    };

                    upper_level_sstables.push(Arc::clone(sstable));
                }

                let mut lower_level_sstables = Vec::new();
                for sstable_id in lower_level_sst_ids {
                    let Some(sstable) = snapshot.sstables.get(sstable_id) else {
                        bail!("sstable with id={sstable_id} is missing");
                    };

                    lower_level_sstables.push(Arc::clone(sstable));
                }

                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_level_sstables)?;

                if upper_level.is_none() {
                    let mut upper_iters = Vec::new();
                    for sstable in upper_level_sstables {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            sstable,
                        )?));
                    }
                    let upper_iter = MergeIterator::create(upper_iters);
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                    self.compact_iter(iter, task.compact_to_bottom_level())
                } else {
                    let upper_iter =
                        SstConcatIterator::create_and_seek_to_first(upper_level_sstables)?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.compact_iter(iter, task.compact_to_bottom_level())
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let state = {
                    let state = self.state.read();
                    Arc::clone(&state)
                };

                let mut l0_sstable_iters = Vec::with_capacity(l0_sstables.len());
                for sstable_id in l0_sstables.iter() {
                    let Some(sstable) = state.sstables.get(sstable_id) else {
                        bail!("sstable with id={sstable_id} missing");
                    };

                    let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sstable))?;
                    l0_sstable_iters.push(Box::new(iter));
                }

                let mut l1_sstables_iter = Vec::with_capacity(l1_sstables.len());
                for sstable_id in l1_sstables.iter() {
                    let Some(sstable) = state.sstables.get(sstable_id) else {
                        bail!("sstable with id={sstable_id} missing");
                    };

                    l1_sstables_iter.push(Arc::clone(sstable));
                }

                let l0_iter = MergeIterator::create(l0_sstable_iters);
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables_iter)?;
                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
                self.compact_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            let l0_sstables = state.l0_sstables.clone();
            let l1_sstables = state.levels[0].1.clone();
            (l0_sstables, l1_sstables)
        };

        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let new_ssts = self.compact(&compaction_task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut snapshot = {
                let state = self.state.read();
                state.as_ref().clone()
            };
            // let mut snapshot = snapshot;
            for l0_sstable_id in l0_sstables.iter().rev() {
                let removed_l0_sstable_id = snapshot.l0_sstables.pop();
                assert!(removed_l0_sstable_id.is_some());
                assert_eq!(*l0_sstable_id, removed_l0_sstable_id.unwrap());
                let removed_l0_sstable = snapshot.sstables.remove(l0_sstable_id);
                assert!(removed_l0_sstable.is_some());
            }

            for l1_sstable_id in l1_sstables.iter() {
                let removed_sstable = snapshot.sstables.remove(l1_sstable_id);
                assert!(removed_sstable.is_some());
            }

            snapshot.levels[0] = (1, new_ssts.iter().map(|sst| sst.sst_id()).collect());

            let mut new_sstable_ids = Vec::new();
            for new_sstable in new_ssts {
                let sst_id = new_sstable.sst_id();
                snapshot.sstables.insert(sst_id, new_sstable);
                new_sstable_ids.push(sst_id);
            }

            self.sync_dir()?;

            self.manifest
                .as_ref()
                .expect("manifest must exist")
                .add_record(
                    &state_lock,
                    ManifestRecord::Compaction(compaction_task, new_sstable_ids),
                )?;

            *self.state.write() = Arc::new(snapshot);
        }

        // for sstable_id in l0_sstables.iter().chain(l1_sstables.iter()) {
        //     std::fs::remove_file(self.path_of_sst(*sstable_id))?;
        // }

        self.sync_dir()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.as_ref().clone()
        };

        let Some(compaction_task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };

        println!("Running compaction task: {:?}", compaction_task);
        let output = self.compact(&compaction_task)?;
        let output_sstable_ids = output
            .iter()
            .map(|sstable| sstable.sst_id())
            .collect::<Vec<_>>();

        let sstable_ids_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();

            let mut new_sstable_ids = Vec::new();
            for sstable in output.clone() {
                new_sstable_ids.push(sstable.sst_id());
                let result = snapshot.sstables.insert(sstable.sst_id(), sstable);
                assert!(result.is_none());
            }

            let (snapshot, sstable_ids_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &compaction_task, &output_sstable_ids, false);

            // for sstable_id in sstable_ids_to_remove.iter() {
            //     let result = snapshot.sstables.remove(sstable_id);
            //     assert!(result.is_some());
            // }

            self.sync_dir()?;

            self.manifest
                .as_ref()
                .expect("manifest must exist")
                .add_record(
                    &state_lock,
                    ManifestRecord::Compaction(compaction_task, new_sstable_ids),
                )?;

            {
                let mut state_guard = self.state.write();
                *state_guard = Arc::new(snapshot);
            }

            sstable_ids_to_remove
        };

        println!(
            "Compaction finished: {} files removed, {} files added, output={:?}",
            sstable_ids_to_remove.len(),
            output.len(),
            output_sstable_ids,
        );

        // for remove_sstable_id in sstable_ids_to_remove {
        //     std::fs::remove_file(self.path_of_sst(remove_sstable_id))?;
        // }

        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_imm_memtables = {
            let state = self.state.read();
            state.imm_memtables.len()
        };

        if num_imm_memtables < self.options.num_memtable_limit {
            return Ok(());
        }

        self.force_flush_next_imm_memtable()
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn compact_iter<I>(&self, mut iter: I, is_bottom_level: bool) -> Result<Vec<Arc<SsTable>>>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut new_sstables = Vec::new();
        let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
        let mut prev_key = crate::key::KeyVec::new();
        let mut new_key;
        let mut seen_version_below_watermark = false;
        let watermark = self.mvcc().watermark();
        while iter.is_valid() {
            new_key = iter.key().key_ref() != prev_key.key_ref();
            if new_key {
                seen_version_below_watermark = false;
            }

            if new_key && sstable_builder.estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                new_sstables.push(Arc::new(sstable_builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));

                sstable_builder = SsTableBuilder::new(self.options.block_size);
            }

            prev_key.set_from_slice(iter.key());

            let remove_entry = (seen_version_below_watermark && iter.key().ts() <= watermark)
                || (is_bottom_level && iter.value().is_empty() && iter.key().ts() == watermark);

            if iter.key().ts() <= watermark {
                seen_version_below_watermark = true;
            }

            if !remove_entry {
                sstable_builder.add(iter.key(), iter.value());
            }

            iter.next()?;
        }

        if sstable_builder.estimated_size() > 0 {
            let id = self.next_sst_id();
            new_sstables.push(Arc::new(sstable_builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }

        Ok(new_sstables)
    }
}
