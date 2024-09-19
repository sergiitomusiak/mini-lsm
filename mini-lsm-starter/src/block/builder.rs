#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let left_space = (self.block_size as isize - self.data.len() as isize) - 2;
        let required_space = (key.len() + value.len() + 6) as isize;
        let exceeds_block_size = required_space > left_space;

        if !self.data.is_empty() && exceeds_block_size {
            return false;
        }

        let (key_overlap_len, rest_key_len) = if self.data.is_empty() {
            self.first_key = key.to_key_vec();
            (0, key.len() as u16)
        } else {
            let mut key_overlap_len = 0;
            let first_key = self.first_key.raw_ref();
            let key_raw_ref = key.raw_ref();
            for i in 0..self.first_key.len() {
                if first_key[i] != key_raw_ref[i] {
                    key_overlap_len = i;
                    break;
                }
            }
            let rest_key_len = key.len() - key_overlap_len;
            (key_overlap_len as u16, rest_key_len as u16)
        };

        self.offsets.push(self.data.len() as u16);
        // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
        self.data.extend_from_slice(&key_overlap_len.to_be_bytes());
        self.data.extend_from_slice(&rest_key_len.to_be_bytes());
        self.data
            .extend_from_slice(&key.raw_ref()[key_overlap_len as usize..]);

        let val_len = value.len() as u16;
        self.data.extend_from_slice(&val_len.to_be_bytes());
        self.data.extend_from_slice(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
