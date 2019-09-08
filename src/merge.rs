use failure::Error;

use crate::count::WordCountOffset;


/// MergeCounter works like reduce
pub struct MergeCounter {
    inner: Vec<WordCountOffset>,

    pub ans: Option<(String, u64)>,
}

impl MergeCounter {
    pub fn new() -> Result<Self, Error> {
        Ok(MergeCounter {
            inner: Vec::new(),
            ans: None,
        })
    }

    /// count with offset
    pub fn count(&mut self, key: String, other_count: u64, offset: u64) {
        let item = self.inner.last_mut();

        match item {
            Some(item) => {
                if item.0 == key {
                    // 重复出现的单词, 合并计数.
                    // 由于重复出现, offset 不需要再考虑了.
                    item.1 += other_count
                } else {
                    // 最后一个元素和当前插入的单词不等的话, 那最后一个元素可以被删除
                    let last = self.inner.pop().unwrap();

                    if last.1 == 1 {
                        // 如果只出现一次, 比较 offset
                        match &mut self.ans {
                            Some((word, offset)) => {
                                if last.2 < *offset {
                                    *word = last.0;
                                    *offset = last.2;
                                }
                            }

                            None => {
                                self.ans = Some((last.0, last.2));
                            }
                        }
                    } else {
                        // 出现多次的元素直接删除
                    }

                    self.inner.push(WordCountOffset(key, other_count, offset))
                }
            }
            None => self.inner.push(WordCountOffset(key, other_count, offset)),
        }
    }

    pub fn get_ans(&mut self) -> Option<(String, u64)> {
        // 合并完成后至多存在一个元素
        let last = self.inner.pop();

        match last {
            Some(wco) => {
                if wco.1 == 1 {
                    // 与 self.ans 作比较选择 offset 最小的

                    match &mut self.ans {
                        Some((word, offset)) => {
                            if wco.2 < *offset {
                                *word = wco.0;
                                *offset = wco.2;
                            }
                        }

                        None => self.ans = Some((wco.0, wco.2)),
                    }
                }
            }
            None => {}
        }

        self.ans.clone()
    }
}
