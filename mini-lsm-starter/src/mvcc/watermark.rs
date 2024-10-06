use std::collections::{btree_map::Entry, BTreeMap};

#[derive(Default)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_default() += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let Entry::Occupied(mut entry) = self.readers.entry(ts) else {
            panic!("reader does not exist for timestamp {ts}");
        };

        if *entry.get() > 1 {
            *entry.get_mut() -= 1;
        } else {
            let value = entry.remove();
            assert_eq!(value, 1);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(key, _)| *key)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
