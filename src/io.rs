use failure::Error;
use failure::Fail;

use std::fs::File;
use std::io::prelude::*;

use std::path::Path;

const CHUNK_SIZE: u64 = 1024 * 1024 * 1024;

pub struct ChunkFile {
    file: File,

    chunk_pos: usize,

    need_read: bool,
    is_end: bool,

    file_size: u64,
    load_size: u64,

    chunk_size: usize,
    chunk: Vec<u8>,
}

#[derive(Fail, Debug)]
pub enum ChunkError {
    #[fail(display = "need next chunk")]
    NextChunk,
    #[fail(display = "eof")]
    Eof,

    #[fail(display = "")]
    IoError,
}

impl ChunkFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = File::open(path)?;
        let meta = file.metadata().unwrap();

        let mut chunk_file = ChunkFile {
            file,
            chunk_pos: 0,
            chunk_size: 0,
            need_read: false,
            is_end: false,
            file_size: meta.len(),
            load_size: 0,
            chunk: vec![0u8; CHUNK_SIZE as usize],
        };

        chunk_file.init()?;

        Ok(chunk_file)
    }

    /// load first chunk into memory
    fn init(&mut self) -> Result<usize, Error> {
        let size = self.file.read(&mut self.chunk)?;

        self.load_size += size as u64;
        self.chunk_size = size;
        self.is_end = self.load_size >= self.file_size;

        Ok(size)
    }

    /// try a new chunk.
    /// preserve unprocessed bytes
    pub fn load_chunk(&mut self) -> Result<usize, ChunkError> {
        let size = if self.chunk_pos < self.chunk.len() {
            let remain = self.chunk[self.chunk_pos..self.chunk_size].to_owned();

            for (idx, c) in remain.into_iter().enumerate() {
                self.chunk[idx] = c;
            }

            self.file.read(&mut self.chunk[self.chunk_pos..])
        } else {
            self.file.read(&mut self.chunk)
        }
            .map_err(|_| ChunkError::IoError)?;

        self.load_size += size as u64;
        self.chunk_size = size;

        self.chunk_pos = 0;

        self.is_end = self.load_size >= self.file_size;

        Ok(size)
    }

    /// try read next line, with offset in origin file
    fn next_line(&mut self) -> Result<(Vec<u8>, u64), ChunkError> {
        let mut word = Vec::new();
        let offset = (self.load_size / CHUNK_SIZE) * CHUNK_SIZE + self.chunk_pos as u64;

        for &byte in self.chunk[self.chunk_pos..self.chunk_size].iter() {
            word.push(byte);

            if byte == b'\n' {
                break;
            }
        }

        Ok((word, offset as u64))
    }

    /// return next `word` in current chunk
    /// `word` must end with `\n` unless last chunk
    ///
    /// if the word is not end with `\n`, a [ChunkError::NextChunk] may return.
    ///
    /// call [next_chunk] to load next chunk into memory
    pub fn next_word(&mut self) -> Result<(String, u64), ChunkError> {
        let (mut word, offset) = self.next_line()?;

        self.chunk_pos += word.len();

        if word.is_empty() {
            if self.is_end {
                return Err(ChunkError::Eof);
            } else {
                self.need_read = true;
                return Err(ChunkError::NextChunk);
            }
        } else {
            let last = word.last().unwrap();

            if *last == b'\n' {
                word.pop().unwrap();    // trim
                return Ok((String::from_utf8(word).unwrap(), offset));
            } else {
                // the file may not end with newline, thus this is the last line
                // otherwise a new chunk is required
                if self.is_end {
                    return Ok((String::from_utf8(word).unwrap(), offset));
                } else {
                    self.need_read = true;
                    return Err(ChunkError::NextChunk);
                }
            }
        }
    }
}