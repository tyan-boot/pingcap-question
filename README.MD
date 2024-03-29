# PingCap 小作业

## 描述

有一个 100GB 的文件，里面内容是文本，要求：

* 找出第一个不重复的词
* 只允许扫一遍原文件
* 尽量少的 IO
* 内存限制 16G

## 一些思路

一开始有考虑直接用 `HashMap` 来计数, 考虑到容易出现所有单词都不重复的情况, 并且这种情况还很容易出现, 例如若所有 `单词` 均是 4 bytes
长度, 则 `INT_MAX * 4` 已经有 16Gb 大小了.

于是试图只靠内存就完成不太现实. 于是思考了下利用磁盘来分块处理文件, 最后合并起来的方案.

### 分块统计
对于源文件, 分割成 N 个不超过 M 大小的子文件, 分别对 N 个文件进行单词计数, 并将计数好的结果按照单词进行排序, 写入到磁盘中. 由于需要
寻找第一个不重复的单词, 因此还需要记录一个 `offset`, 即这个单词最初出现的位置.

因此用 `HashMap` 来计数的话, 最好的情况下不会出现哈希冲突的话, 所需要的空间就是

```
M 大小的单词 + (u64(单词次数) + u64(offset)) * 单词个数
```

极端情况下 100Gb 里面每个单词都只有一个字符, 那么 M 不能超过 1Gb. 所以最后决定分块大小是 1Gb.

当分块统计好后, 将 `map entry` 按照 `key` 排序后序列化到磁盘文件中. 

### 合并
分块统计好之后, N 个临时文件当中都是已经排好序的, 对这 N 个文件同时进行归并.

一开始还是考虑用 `HashMap` 来完成合并操作, 并且在适当的情况下, 也就是 `HashMap` 占用内存过大的情况下写入到磁盘中.

不过想了下后意识到, 既然所有文件都是已经排好序的, 那在归并的时候, 按照每个临时文件首行单词进行排序, 来选择下一个合并的单词.

这样的话如果下一个插入到 `map` 中的单词和上一次插入到 `map` 中的单词是不同的话, 就意味着上一次插入的单词在所有临时文件中就不会再出现了.

而如果恰好 `last_item` 的计数是大于 1 的话, 是可以直接过滤掉的.

于是将 `map` 换成了 `vec` 来存, 每次执行插入操作的时候取出 `vec` 的最后一项, 若相同, 则更新单词计数.

若不相同, 则根据最后一项的 `count` 来决定直接从 `vec` 中删除, 还是和 `ans` 比较 offset来决定哪个是第一个不重复的单词.

这样实际上 `vec` 中至多只会有一个元素.

在最后合并结束之后, 就可以直接取得第一个不重复的单词了.

## 代码结构

|   mod    |            description             |
| -------- | ---------------------------------- |
| count.rs | `BTreeMap` 计数,并且flush 到磁盘文件 |
| io.rs    | 分块读取源文件, 并且按行返回          |
| merge.rs | 合并计数, 同时记录最早出现的不重复单词 |
| main.rs  | 调度分块和合并                       |

### 存在的问题

* M 的大小取 1Gb 也是不合理的, 文件 `buff` 本身也会占用内存, 并且 `map` 占用的内存可能并不是准确计算的.
* 只在读源文件的时候有 `buff`, 在读写临时文件的时候只用了较小的缓存, 内存利用率较低.


## 第二版思路
之前在分块储存的时候需要排序, 排序的目的是为了方便后续的合并, 然而合并是因为没有办法确定一个单词是否在其他
文件中出现过, 所以必须合并之后才能知道哪些是不重复的.

如果能够在一开始分块的时候就能确保同样的单词不会出现在不同文件中, 就可以避免排序和合并的阶段.

于是考虑用 `Hash` 取模来对源文件进行分块, 使得一种单词只会出现在一个文件中. 同时还需要尽可能的保证每个分块文件大小都不能太大, 以便于
能够直接放入内存中.

对于分割后大小仍然太大的, 继续用 `Hash` 来分割.

分割完成后, 对每个文件单独进行计数, 计数完成后寻找当前分块中的第一个不重复的单词, 并且存起来. 当所有分块文件都处理完成后, 对每个分块
里的"第一个不重复"排序, 以此来找到源文件中第一个不重复的单词.

### 存在的问题
* 仍然会出现多次分割后依然有分块大小过大的情况.
* 在分块阶段由于需要同时记录偏移量, 因此会写入大于100Gb的临时文件. 而在前一种方案中对于有一定重复单词的情况下, 写入的临时文件是较小的.
可以考虑一边分块一边处理, 只有内存不足的时候从才考虑写入到磁盘中.