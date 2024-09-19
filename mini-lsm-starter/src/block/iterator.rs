#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::Ordering, sync::Arc};

use crate::{
    block::u16_from_offset,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut first_key = KeyVec::new();
        if !block.data.is_empty() {
            assert_eq!(u16_from_offset(&block.data, 0), 0);
            let first_key_len = u16_from_offset(&block.data, 2);
            first_key.append(&block.data[4..4 + first_key_len as usize]);
        }

        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.key.clear();
        self.value_range = (0, 0);
        self.idx = 0;
        self.next();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let idx = if self.key().is_empty() {
            0
        } else {
            self.idx + 1
        };
        self.move_to_index(idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut start = 0;
        let mut end = self.block.offsets.len() - 1;

        while start < end {
            let mid = (start + end) / 2;
            match self.cmp_with_key_at(mid, key.raw_ref()) {
                Ordering::Equal => {
                    self.move_to_index(mid);
                    return;
                }
                Ordering::Less => start = mid + 1,
                Ordering::Greater => end = mid,
            }
        }

        if self.cmp_with_key_at(end, key.raw_ref()) == Ordering::Less {
            end += 1;
        }
        self.move_to_index(end);
    }

    fn move_to_index(&mut self, idx: usize) {
        self.idx = idx;
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }
        let offset = self.block.offsets[self.idx] as usize;
        let key_overlap_len = u16_from_offset(&self.block.data, offset) as usize;
        let rest_key_len = u16_from_offset(&self.block.data, offset + 2) as usize;
        let key_offset = offset + 4;
        self.key.clear();
        self.key
            .append(&self.first_key.raw_ref()[..key_overlap_len]);
        self.key
            .append(&self.block.data[key_offset..key_offset + rest_key_len]);
        let value_len = u16_from_offset(&self.block.data, key_offset + rest_key_len) as usize;
        let value_offset = key_offset + rest_key_len + 2;
        self.value_range = (value_offset, value_offset + value_len);
    }

    fn cmp_with_key_at(&self, entry_idx: usize, other_key: &[u8]) -> Ordering {
        let offset = self.block.offsets[entry_idx] as usize;
        let key_overlap_len = u16_from_offset(&self.block.data, offset) as usize;
        let rest_key_len = u16_from_offset(&self.block.data, offset + 2) as usize;
        let key_offset = offset + 4;
        let mut key = KeyVec::new();
        key.append(&self.first_key.raw_ref()[..key_overlap_len]);
        key.append(&self.block.data[key_offset..key_offset + rest_key_len]);
        key.raw_ref().cmp(other_key)
    }
}
