use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() <= self.options.num_tiers {
            return None;
        }

        // Space amplification trigger
        // let mut upper_tiers_size = snapshot.l0_sstables.len() as f64;
        let mut upper_tiers_size = 0.0;
        for tier in 0..(snapshot.levels.len() - 1) {
            upper_tiers_size += snapshot.levels[tier].1.len() as f64;
        }

        let last_tier_size = snapshot
            .levels
            .last()
            .expect("at least one tier must be present for compaction")
            .1
            .len() as f64;

        if (upper_tiers_size / last_tier_size) * 100.0
            >= self.options.max_size_amplification_percent as f64
        {
            let tiers = snapshot.levels.clone();
            return Some(TieredCompactionTask {
                tiers,
                bottom_tier_included: true,
            });
        }

        // Size ratio trigger
        // let mut previous_tiers_size = snapshot.l0_sstables.len() as f64;
        let mut previous_tiers_size = snapshot.levels[0].1.len() as f64;
        for tier in 1..snapshot.levels.len() {
            let current_tier_size = snapshot.levels[tier].1.len() as f64;
            if tier >= self.options.min_merge_width
                && (previous_tiers_size / current_tier_size) * 100.0
                    >= (100.0 + self.options.size_ratio as f64)
            {
                // Collect previous tiers
                let mut tiers = Vec::new();
                for i in 0..=tier {
                    tiers.push(snapshot.levels[i].clone());
                }

                return Some(TieredCompactionTask {
                    tiers,
                    bottom_tier_included: (tier + 1) == snapshot.levels.len(),
                });
            }
            previous_tiers_size += current_tier_size;
        }

        assert!(snapshot.levels.len() > self.options.num_tiers);
        let tiers_num = snapshot.levels.len() + 1 - self.options.num_tiers;
        let mut tiers = Vec::new();
        for i in 0..tiers_num {
            tiers.push(snapshot.levels[i].clone());
        }

        Some(TieredCompactionTask {
            tiers,
            bottom_tier_included: tiers_num == snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                assert_eq!(ffiles, files, "file changed after issuing compaction task");
                files_to_remove.extend(ffiles.iter().copied());
            } else {
                // retain the tier
                levels.push((*tier_id, files.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                // add the compacted tier to the LSM tree
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }

        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
