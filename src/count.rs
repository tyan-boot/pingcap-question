use failure::Error;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom};

#[derive(Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct WordCountOffset(pub String, pub u64, pub u64);

/// Counter internal using BTreeMap to count word and keep keys ordered
pub struct Counter {
    inner: BTreeMap<String, (u64, u64)>,
}

impl Counter {
    pub fn new() -> Self {
        Counter {
            inner: BTreeMap::new(),
        }
    }

    /// count a word, return the new count
    pub fn count(&mut self, key: String, offset: u64) -> u64 {
        let item = self.inner.get_mut(&key);

        match item {
            Some((count, _offset)) => {
                // offset doesn't need update if it exist.
                *count += 1;

                *count
            }
            None => {
                self.inner.insert(key, (1, offset));
                1
            }
        }
    }

    /// flush current counter state to disk, and clear self
    ///
    /// return file handler to temp file
    pub fn flush(&mut self) -> Result<File, Error> {
        let tmp_file = tempfile::tempfile()?;

        let mut writer = BufWriter::new(tmp_file);

        for (key, (count, offset)) in &self.inner {
            let wco = WordCountOffset(key.clone(), *count, *offset);

            bincode::serialize_into(&mut writer, &wco).unwrap();
        }

        let mut file = writer.into_inner()?;
        // reset seek to begin in case for further read
        file.seek(SeekFrom::Start(0))?;

        Ok(file)
    }
}
