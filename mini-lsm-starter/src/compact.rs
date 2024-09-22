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
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
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
        match task {
            CompactionTask::Leveled(..) => todo!(),
            CompactionTask::Tiered(..) => todo!(),
            CompactionTask::Simple(..) => todo!(),
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

                let mut new_sstables = Vec::new();
                let l0_iter = MergeIterator::create(l0_sstable_iters);
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables_iter)?;
                let mut iter = TwoMergeIterator::create(l0_iter, l1_iter)?;

                let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
                while iter.is_valid() {
                    if sstable_builder.estimated_size() >= self.options.target_sst_size {
                        let id = self.next_sst_id();
                        new_sstables.push(Arc::new(sstable_builder.build(
                            id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(id),
                        )?));

                        sstable_builder = SsTableBuilder::new(self.options.block_size);
                    }

                    if !iter.value().is_empty() {
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
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
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

            for new_sstable in new_ssts {
                let sst_id = new_sstable.sst_id();
                snapshot.sstables.insert(sst_id, new_sstable);
            }

            *state = Arc::new(snapshot);
        }

        for sstable_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sstable_id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
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
}
