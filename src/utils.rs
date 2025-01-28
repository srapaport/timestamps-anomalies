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
) -> Option<Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>> {
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

fn generate_commits(amount_of_window: usize)-> Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>{
    let mut commits = Vec::new();
    for _ in 0..amount_of_window{
        let size = rand::random_range(3..=10);
        let mut random_vec: Vec<i16> = vec![];
        for _ in 0..size {
            random_vec.push(rand::random_range(-1000..=1000));
        }
        let i: usize = rand::random_range(0..100);
        let j: usize = rand::random_range(i+1..i+100);
        let i_date = rand::random_range(946684800..Utc::now().timestamp());
        let j_date = rand::random_range(i_date..i_date+100_000);
        commits.push(((i, DateTime::from_timestamp(i_date,0).unwrap()),(j, DateTime::from_timestamp(j_date,0).unwrap()),random_vec));
    }
    commits
}

fn generate_author()->String{
    const HEX_CHARS: &[u8] = b"0123456789abcdef";
    (0..64)
        .map(|_| {
            let idx = rand::random_range(0..HEX_CHARS.len());
            HEX_CHARS[idx] as char
        })
        .collect()
}

pub fn generate_test_sample(amount_of_author: usize) -> Vec<(String, Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>)>{
    let mut test = Vec::new();
    for _ in 0..amount_of_author{
        let amount_of_window = rand::random_range(1..=100);
        test.push((generate_author(), generate_commits(amount_of_window)));
    }
    test
}