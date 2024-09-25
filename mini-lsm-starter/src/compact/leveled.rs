use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut actual_level_sizes = Vec::new();
        for (_, sstable_ids) in snapshot.levels.iter() {
            let size = sstable_ids
                .iter()
                .map(|sstable_id| {
                    snapshot
                        .sstables
                        .get(sstable_id)
                        .expect("sstable must exist")
                        .table_size()
                })
                .sum::<u64>() as usize;

            actual_level_sizes.push(size);
        }

        let mut lower_level = self.options.max_levels;
        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;

        let mut target_level_sizes = vec![0usize; snapshot.levels.len()];
        target_level_sizes[snapshot.levels.len() - 1] =
            actual_level_sizes[snapshot.levels.len() - 1].max(base_level_size);

        for i in (0..(target_level_sizes.len() - 1)).rev() {
            if target_level_sizes[i + 1] > base_level_size {
                target_level_sizes[i] =
                    target_level_sizes[i + 1] / self.options.level_size_multiplier;
            }
            if target_level_sizes[i] > 0 {
                lower_level = i + 1;
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    lower_level,
                ),
                is_lower_level_bottom_level: lower_level >= self.options.max_levels,
            });
        }

        let mut upper_level: Option<(f64, usize)> = None;
        for i in 0..self.options.max_levels {
            let ratio = actual_level_sizes[i] as f64 / target_level_sizes[i] as f64;
            if ratio > 1.0
                && upper_level
                    .as_ref()
                    .map(|upper_level| upper_level.0 < ratio)
                    .unwrap_or(true)
            {
                upper_level = Some((ratio, i + 1));
            }
        }

        let (_, upper_level) = upper_level?;

        let upper_level_sstable = *snapshot.levels[upper_level - 1]
            .1
            .iter()
            .min()
            .expect("upper level sstable must not be empty");

        Some(LeveledCompactionTask {
            upper_level: Some(upper_level),
            upper_level_sst_ids: vec![upper_level_sstable],
            lower_level: upper_level + 1,
            lower_level_sst_ids: self.find_overlapping_ssts(
                snapshot,
                &[upper_level_sstable],
                upper_level + 1,
            ),
            is_lower_level_bottom_level: (upper_level + 1) >= self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut upper_level_sstable_ids = task.upper_level_sst_ids.iter().collect::<HashSet<_>>();
        let mut lower_level_sstable_ids = task.lower_level_sst_ids.iter().collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            let upper_level_sstables = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|sstable_id| {
                    (!upper_level_sstable_ids.remove(sstable_id)).then_some(*sstable_id)
                })
                .collect::<Vec<_>>();

            snapshot.levels[upper_level - 1].1 = upper_level_sstables;
        } else {
            // L0 table compaction
            let l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter_map(|sstable_id| {
                    (!upper_level_sstable_ids.remove(sstable_id)).then_some(*sstable_id)
                })
                .collect::<Vec<_>>();

            snapshot.l0_sstables = l0_sstables;
        }

        assert!(upper_level_sstable_ids.is_empty());

        let mut new_lower_level_sstables = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|sstable_id| {
                (!lower_level_sstable_ids.remove(sstable_id)).then_some(*sstable_id)
            })
            .collect::<Vec<_>>();

        assert!(lower_level_sstable_ids.is_empty());
        new_lower_level_sstables.extend(output);

        if !in_recovery {
            new_lower_level_sstables.sort_by(|sstable_1, sstable_2| {
                snapshot
                    .sstables
                    .get(sstable_1)
                    .expect("sstable must exist")
                    .first_key()
                    .cmp(
                        snapshot
                            .sstables
                            .get(sstable_2)
                            .expect("sstable must exist")
                            .first_key(),
                    )
            });
        }
        snapshot.levels[task.lower_level - 1].1 = new_lower_level_sstables;

        let mut sstables_to_remove = Vec::new();
        sstables_to_remove.extend_from_slice(&task.lower_level_sst_ids);
        sstables_to_remove.extend_from_slice(&task.upper_level_sst_ids);

        (snapshot, sstables_to_remove)
    }
}
