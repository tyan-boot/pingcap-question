use std::fs::File;

use failure::Error;
use std::collections::HashMap;
use std::io::{Read, BufReader, BufRead};
use crate::v2::io::WordOffset;
use bincode::ErrorKind;

pub struct Counter {
    chunks: Vec<File>,
    map: HashMap<String, (u64, u64)>,

    ans: Vec<(String, u64)>,
}

impl Counter {
    pub fn new(chunks: Vec<File>) -> Result<Self, Error> {
        Ok(Counter {
            chunks,
            map: HashMap::new(),
            ans: Vec::new(),
        })
    }

    pub fn count(&mut self, word: String, offset: u64) {
        let mut item = self.map.get_mut(&word);

        match item {
            Some((count, offset)) => {
                *count += 1
            }
            None => {
                self.map.insert(word, (1, offset));
            }
        }
    }

    pub fn rotate(&mut self) {
        let mut ans: Option<(String, u64)> = None;

        for (word, (count, offset)) in self.map.iter() {
            if *count != 1 {
                continue;
            }

            match &mut ans {
                Some((ans_word, ans_offset)) => {
                    if offset < ans_offset {
                        *ans_word = word.clone();
                        *ans_offset = *offset;
                    }
                }
                None => {
                    ans = Some((word.clone(), *offset));
                }
            }
        }

        if let Some(ans) = ans {
            self.ans.push(ans);
        }

        self.map.clear();
    }

    pub fn run(&mut self) {
        while let Some(mut chunk) = self.chunks.pop() {
            let mut buff = Vec::new();
            chunk.read_to_end(&mut buff);

            let mut reader = BufReader::new(&*buff);

            loop {
                let wo: Result<WordOffset, Box<ErrorKind>> = bincode::deserialize_from(&mut reader);

                match wo {
                    Ok(wo) => {
                        self.count(wo.0, wo.1);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }

            self.rotate();
        }
    }

    pub fn finish(mut self) -> Option<(String, u64)> {
        self.ans.sort_by(|lhs, rhs| {
            rhs.1.cmp(&lhs.1)
        });

        self.ans.pop()
    }
}