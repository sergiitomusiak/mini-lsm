#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    lsm_storage::key_within,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    // next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(mut sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        sstables.reverse();
        let iter = Self {
            current: sstables
                .pop()
                .map(SsTableIterator::create_and_seek_to_first)
                .transpose()?,
            sstables,
        };

        Ok(iter)
    }

    pub fn create_and_seek_to_key(mut sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut sstable_idx = sstables.len();
        for (i, sstable) in sstables.iter().enumerate() {
            let is_key_within = key_within(
                &key.key_ref(),
                Bound::Unbounded,
                Bound::Excluded(sstable.first_key().key_ref()),
            ) || key_within(
                key.key_ref(),
                Bound::Included(sstable.first_key().key_ref()),
                Bound::Included(sstable.last_key().key_ref()),
            );

            if is_key_within {
                sstable_idx = i;
                break;
            }
        }

        let mut sstables = sstables.split_off(sstable_idx);
        sstables.reverse();
        let iter = Self {
            current: sstables
                .pop()
                .map(|sstable| SsTableIterator::create_and_seek_to_key(sstable, key))
                .transpose()?,
            sstables,
        };

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        while self.current.is_some() {
            if self.current.as_ref().unwrap().is_valid() {
                break;
            }

            self.current = self
                .sstables
                .pop()
                .map(SsTableIterator::create_and_seek_to_first)
                .transpose()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
