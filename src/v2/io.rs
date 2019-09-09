use std::fs::File;
use std::io::{Read, BufReader, BufRead, Write, Seek, SeekFrom};

use super::utils::hash;
use failure::Error;
use std::process::id;
use crate::v1::io::{ChunkFile, ChunkError, DEFAULT_CHUNK_SIZE};
use std::path::Path;
use serde::{Serialize, Deserialize};
use bincode::ErrorKind;

const CHUNK_COUNT: u64 = 50;
const CHUNK_THRESHOLD: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
pub struct WordOffset(pub String, pub u64);

pub struct HashSplitFile {
    inner: ChunkFile,
    chunks: Vec<File>,

    big_chunks: Vec<File>,
}

impl HashSplitFile
{
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let chunk_file = ChunkFile::new(path, DEFAULT_CHUNK_SIZE)?;
        let mut chunks = Vec::with_capacity(CHUNK_COUNT as usize);

        for _ in 0..CHUNK_COUNT {
            let chunk = tempfile::tempfile()?;
            chunks.push(chunk);
        }

        Ok(HashSplitFile {
            inner: chunk_file,
            chunks,
            big_chunks: Vec::new(),
        })
    }

    fn split_big_chunks(&mut self) {
        let mut chunks = Vec::new();

        while let Some(mut file) = self.big_chunks.pop() {
            let metadata = file.metadata().unwrap();
            let count = metadata.len() / CHUNK_THRESHOLD + 1;

            let mut part_chunks = Vec::with_capacity(count as usize);

            for _ in 0..count {
                part_chunks.push(tempfile::tempfile().unwrap());
            }

            let mut reader = BufReader::new(file);

            loop {
                let wo: Result<WordOffset, Box<ErrorKind>> = bincode::deserialize_from(&mut reader);

                match wo {
                    Ok(wo) => {
                        let h = hash(&wo.0);
                        let idx = h % count;

                        let mut chunk = &part_chunks[idx as usize];
                        bincode::serialize_into(&mut chunk, &wo);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }

            chunks.append(&mut part_chunks);
        }

        chunks.iter()
            .for_each(|mut it| {
                it.seek(SeekFrom::Start((0)));
            });

        while let Some(file) = chunks.pop() {
            let metadata = file.metadata().unwrap();
            if metadata.len() > CHUNK_THRESHOLD {
                self.big_chunks.push(file)
            } else {
                self.chunks.push(file)
            }
        }
    }

    pub fn split(&mut self) -> Result<(), Error> {
        loop {
            let line = self.inner.next_word();

            match line {
                Ok((line, offset)) => {
                    let h = hash(&line);
                    let idx = h % CHUNK_COUNT;

                    let wo = WordOffset(line, offset);
                    let mut file = &self.chunks[idx as usize];

                    let buf = bincode::serialized_size(&wo);

                    bincode::serialize_into(&mut file, &wo);
                }

                Err(ChunkError::NextChunk) => {
                    self.inner.load_chunk()?;
                }

                Err(ChunkError::Eof) => {
                    // end of file
                    break;
                }

                Err(ChunkError::IoError) => panic!(),
            }
        }

        self.chunks.iter()
            .for_each(|mut it| {
                it.seek(SeekFrom::Start((0)));
            });

        let mut chunks = Vec::new();

        while let Some(file) = self.chunks.pop() {
            let metadata = file.metadata().unwrap();
            if metadata.len() > CHUNK_THRESHOLD {
                self.big_chunks.push(file)
            } else {
                chunks.push(file)
            }
        }

        self.chunks.append(&mut chunks);

        if !self.big_chunks.is_empty() {
            self.split_big_chunks();
        }

        Ok(())
    }

    pub fn finish(self) -> Vec<File> {
        self.chunks
    }
}