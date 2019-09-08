use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use bincode::ErrorKind;
use failure::Error;

use crate::count::Counter;
use crate::count::WordCountOffset;
use crate::io::{ChunkError, ChunkFile, DEFAULT_CHUNK_SIZE};
use crate::merge::MergeCounter;

mod count;
mod io;
mod merge;

struct MergePair(String, u64, u64, BufReader<File>);

struct Count {
    io: ChunkFile,

    counter: Counter,
    merger: MergeCounter,
    chunks: Vec<BufReader<File>>,
}

impl Count {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Count, Error> {
        let io = ChunkFile::new(path, DEFAULT_CHUNK_SIZE)?;

        let counter = Counter::new();

        Ok(Count {
            io,
            counter,
            merger: MergeCounter::new().unwrap(),
            chunks: Vec::new(),
        })
    }

    /// split file into chunk and count part by part
    fn count_chunk(&mut self) -> Result<(), Error> {
        loop {
            let word = self.io.next_word();

            match word {
                Ok((word, offset)) => {
                    self.counter.count(word, offset);
                }

                Err(ChunkError::NextChunk) => {
                    // flush counter to tmp file
                    let file = self.counter.flush()?;
                    let reader = BufReader::new(file);

                    // and load new chunk
                    self.chunks.push(reader);

                    self.io.load_chunk()?;
                }

                Err(ChunkError::Eof) => {
                    // end of file
                    break;
                }

                Err(ChunkError::IoError) => panic!(),
            }
        }

        // keep handler for merge
        let file = self.counter.flush()?;
        let reader = BufReader::new(file);

        self.chunks.push(reader);

        Ok(())
    }

    /// merge all temp file simultaneously
    fn merge(&mut self) {
        let mut queue = Vec::new();

        // for each file get first line
        while let Some(mut file) = self.chunks.pop() {
            let wco: WordCountOffset = bincode::deserialize_from(&mut file).unwrap();
            queue.push(MergePair(wco.0, wco.1, wco.2, file));
        }

        loop {
            // sort files using first line
            queue.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));

            let next = queue.pop();

            match next {
                Some(pair) => {
                    // merge
                    self.merger.count(pair.0, pair.1, pair.2);

                    let mut file = pair.3;

                    let wco: Result<WordCountOffset, Box<ErrorKind>> =
                        bincode::deserialize_from(&mut file);

                    // enqueue if temp file is not empty
                    match wco {
                        Ok(wco) => {
                            queue.push(MergePair(wco.0, wco.1, wco.2, file));
                        }
                        Err(_e) => {
                            // todo: check eof
                            continue;
                        }
                    }
                }
                None => {
                    // all temp file processed
                    break;
                }
            }
        }
    }

    pub fn solve(&mut self) -> Option<String> {
        self.count_chunk().ok()?;

        self.merge();

        self.merger.get_ans().map(|it| it.0)
    }
}

fn main() {
    let mut count = Count::new("test.txt").unwrap();
    let ans = count.solve();

    dbg!(ans);
}
