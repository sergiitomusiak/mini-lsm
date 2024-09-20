#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::KeySlice,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iter = Self {
            table,
            blk_idx: 0,
            blk_iter: BlockIterator::create_and_seek_to_first(Arc::new(Block::empty())),
        };
        iter.set_block(0, None)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.set_block(0, None)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            table,
            blk_idx: 0,
            blk_iter: BlockIterator::create_and_seek_to_first(Arc::new(Block::empty())),
        };
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut start = 0;
        let mut end = self.table.block_meta.len() - 1;
        while (end - start) > 1 {
            let mid = (start + end) / 2;
            match self.table.block_meta[mid]
                .first_key
                .as_key_slice()
                .cmp(&key)
            {
                Ordering::Equal => {
                    start = mid;
                    break;
                }
                Ordering::Greater => end = mid,
                Ordering::Less => start = mid,
            }
        }

        let block_idx = if self.table.block_meta[start].last_key.as_key_slice() >= key {
            start
        } else {
            end
        };

        self.set_block(block_idx, Some(key))?;
        Ok(())
    }

    fn set_block(&mut self, block_idx: usize, key: Option<KeySlice>) -> Result<()> {
        let block = self.table.read_block_cached(block_idx)?;
        self.blk_iter = if let Some(key) = key {
            BlockIterator::create_and_seek_to_key(block, key)
        } else {
            BlockIterator::create_and_seek_to_first(block)
        };
        self.blk_idx = block_idx;
        Ok(())
    }

    fn ensure_block_iter_valid(&mut self) -> Result<()> {
        if self.blk_idx < (self.table.block_meta.len() - 1) && !self.is_valid() {
            self.set_block(self.blk_idx + 1, None)?;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        self.ensure_block_iter_valid()?;
        Ok(())
    }
}
