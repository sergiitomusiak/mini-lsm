use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        for level_index in 1..self.options.max_levels {
            if snapshot.levels[level_index - 1].1.is_empty() {
                continue;
            }

            let size_ratio = ((snapshot.levels[level_index].1.len() as f64)
                / (snapshot.levels[level_index - 1].1.len() as f64))
                * 100.0;
            let level = level_index + 1;
            if (size_ratio as usize) < self.options.size_ratio_percent {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level - 1),
                    upper_level_sst_ids: snapshot.levels[level_index - 1].1.clone(),
                    lower_level: level,
                    lower_level_sst_ids: snapshot.levels[level_index].1.clone(),
                    is_lower_level_bottom_level: (level + 1) == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut remove_sstables = Vec::new();
        if let Some(upper_level) = task.upper_level {
            snapshot.levels[upper_level - 1].1.clear();
            remove_sstables.extend_from_slice(&task.upper_level_sst_ids);
        } else {
            let mut compated_l0_sstables = task.upper_level_sst_ids.clone();
            compated_l0_sstables.reverse();

            for compacted_l0_sstable_id in compated_l0_sstables.iter() {
                let removed_l0_sstable_id = snapshot.l0_sstables.pop();
                assert!(removed_l0_sstable_id.is_some());
                assert_eq!(removed_l0_sstable_id.unwrap(), *compacted_l0_sstable_id);
            }

            remove_sstables.extend_from_slice(&compated_l0_sstables);
        }

        assert_eq!(
            task.lower_level_sst_ids,
            snapshot.levels[task.lower_level - 1].1,
        );

        remove_sstables.extend_from_slice(&task.lower_level_sst_ids);
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();

        (snapshot, remove_sstables)
    }
}
