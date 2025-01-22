use crate::env;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
// use indicatif::MultiProgress;
use rocksdb::{Options, DB};

pub fn amount_of_commits(opts: &env::Options, author: String) -> usize {
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    match db.get(author) {
        Ok(val) => match val {
            Some(res) => {
                let commits: HashSet<(String, i64, i16)> = bincode::deserialize(&res[..]).unwrap();
                return commits.len();
            }
            None => return 0,
        },
        Err(_) => return 0,
    }
}

pub fn single_author_window_res(
    opts: &env::Options,
    author: String,
) -> Option<Vec<Vec<(String, DateTime<Utc>, i16)>>> {
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    match db.get(author) {
        Ok(val) => match val {
            Some(res) => {
                let commits: Vec<(String, i64, i16)> = bincode::deserialize(&res[..]).unwrap();
                /* let commits: Vec<(String, DateTime<Utc>, i16)> = commits.into_iter().map(|commit|{
                    (commit.0, DateTime::from_timestamp(commit.1, 0).unwrap(), commit.2)
                }).collect(); */
                return Some(crate::window::window_tz_inconsistency_extraction(
                    commits,
                    env::ONE_DAY,
                    // &MultiProgress::new(),
                ));
            }
            None => return None,
        },
        Err(_) => return None,
    }
}
