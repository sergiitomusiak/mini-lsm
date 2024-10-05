#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    block_first_key: KeyVec,
    key_hashes: Vec<u32>,
    estimate_size: usize,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            block_first_key: KeyVec::new(),
            key_hashes: Vec::new(),
            estimate_size: 0,
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        } else {
            assert!(self.first_key.as_key_slice() < key);
        }

        if !self.last_key.is_empty() {
            assert!(self.last_key.as_key_slice() < key);
        }

        if self.block_first_key.is_empty() {
            self.block_first_key.set_from_slice(key);
        } else {
            assert!(self.block_first_key.as_key_slice() < key);
        }

        if key.ts() > self.max_ts {
            self.max_ts = self.max_ts.max(key.ts());
        }

        let added = self.builder.add(key, value);
        if !added {
            self.reset_block_builder();
            self.block_first_key.set_from_slice(key);
            let added = self.builder.add(key, value);
            assert!(added);
        }

        self.estimate_size += key.raw_len() + value.len();
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.estimate_size
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.reset_block_builder();
        }
        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, self.max_ts, &mut self.data);
        let block_meta_offset_bytes = u32::to_be_bytes(block_meta_offset as u32);
        self.data.extend_from_slice(&block_meta_offset_bytes);

        let bloom_bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bloom_bits_per_key);
        let bloom_filter_offset_bytes = u32::to_be_bytes(self.data.len() as u32);
        bloom.encode(&mut self.data);
        self.data.extend_from_slice(&bloom_filter_offset_bytes);

        let file = FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(self.first_key.key_ref()),
                self.first_key.ts(),
            ),
            last_key: KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(self.last_key.key_ref()),
                self.last_key.ts(),
            ),
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    fn reset_block_builder(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build().encode();
        let offset = self.data.len();
        self.data.extend_from_slice(&block);

        let meta = BlockMeta {
            offset,
            first_key: std::mem::take(&mut self.block_first_key).into_key_bytes(),
            last_key: self.last_key.clone().into_key_bytes(),
        };
        self.meta.push(meta);
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
