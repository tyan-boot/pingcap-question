use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use bincode::ErrorKind;
use clap::{App, Arg};
use failure::Error;

mod v1;
mod v2;


fn main() {
    let app = App::new("first-non-repeating word")
        .version("0.1.0")
        .author("tyan boot")
        .arg(Arg::with_name("file").help("input file").required(true).takes_value(true));

    let matches = app.get_matches();

    let input = matches.value_of("file").unwrap();


    use crate::v2::count::Counter;
    use crate::v2::io::HashSplitFile;

    let mut spliter = HashSplitFile::new(input).unwrap();
    spliter.split().unwrap();
    let chunks = spliter.finish();

    let mut counter = Counter::new(chunks).unwrap();

    counter.run();
    let ans = counter.finish();

    dbg!(ans);
//    let mut count = Count::new(input).unwrap();
//    let ans = count.solve();
//
//    dbg!(ans);
}
