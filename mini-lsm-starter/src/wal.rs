#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut data = bytes::Bytes::from(data);
        while data.has_remaining() {
            let key_len = data.get_u16() as usize;
            let key = Bytes::copy_from_slice(&data.chunk()[..key_len]);
            data.advance(key_len);

            let val_len = data.get_u16() as usize;
            let value = Bytes::copy_from_slice(&data.chunk()[..val_len]);
            data.advance(val_len);

            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        Self::put_inner(&mut file, key, value)
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(&[u8], &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        for (key, value) in data {
            Self::put_inner(&mut file, key, value)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }

    fn put_inner(writer: &mut BufWriter<File>, key: &[u8], value: &[u8]) -> Result<()> {
        let key_len = (key.len() as u16).to_be_bytes();
        let val_len = (value.len() as u16).to_be_bytes();

        writer.write_all(&key_len)?;
        writer.write_all(key)?;
        writer.write_all(&val_len)?;
        writer.write_all(value)?;

        Ok(())
    }
}
