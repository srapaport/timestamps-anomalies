use crate::env;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
// use indicatif::MultiProgress;
use log::info;
use rocksdb::{Options, DB};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
};

pub fn create_cp(path_res: &str, cp: &mut Vec<String>) {
    let filename = format!("{}/checkpoints", path_res);
    if let Err(_) = fs::metadata(&filename) {
        File::create_new(&filename)
            .expect(format!("lib.rs > create_cp | File {} already exists!", &filename).as_str());
    }
    let mut cp_file = fs::OpenOptions::new()
        .append(true)
        .open(filename)
        .expect(format!("cannot open checkpoints file").as_str());
    cp.iter().for_each(|author| {
        cp_file
            .write(format!("{}\n", author).as_bytes())
            .expect("couldn't write data in checkpoints");
    });
    cp.clear();
}

pub fn load_checkpoint(path_res: &str) -> Option<Vec<String>> {
    let mut res = Vec::new();

    if let Ok(cp_file) = File::open(format!("{}/checkpoints", path_res)) {
        BufReader::new(cp_file)
            .lines()
            .into_iter()
            .for_each(|line| {
                if let Ok(author) = line {
                    res.push(author);
                }
            });
    } else {
        info!("couldn't open checkpoints file");
        return None;
    }

    return Some(res);
}

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

pub fn single_author_window_res(opts: &env::Options, author: String) -> Option<Vec<env::Window>> {
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    match db.get(author) {
        Ok(val) => match val {
            Some(res) => {
                let commits: env::Revisions = bincode::deserialize(&res[..]).unwrap();
                /* let commits: Vec<(String, DateTime<Utc>, i16)> = commits.into_iter().map(|commit|{
                    (commit.0, DateTime::from_timestamp(commit.1, 0).unwrap(), commit.2)
                }).collect(); */
                return Some(crate::window::window_tz_inconsistency_extraction(
                    commits.revisions,
                    env::ONE_DAY,
                    // &MultiProgress::new(),
                ));
            }
            None => return None,
        },
        Err(_) => return None,
    }
}

pub fn single_author_flight_res(opts: &env::Options, author: String) -> Option<Vec<env::Flight>> {
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    match db.get(author) {
        Ok(val) => match val {
            Some(res) => {
                let commits: Vec<(String, i64, i16, String)> =
                    bincode::deserialize(&res[..]).unwrap();
                /* let commits: Vec<(String, DateTime<Utc>, i16)> = commits.into_iter().map(|commit|{
                    (commit.0, DateTime::from_timestamp(commit.1, 0).unwrap(), commit.2)
                }).collect(); */
                return Some(crate::travel::travel_time(commits));
            }
            None => return None,
        },
        Err(_) => return None,
    }
}

pub fn single_author_commits(
    opts: &env::Options,
    author: String,
) -> Option<Vec<(String, i64, i16)>> {
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    match db.get(author) {
        Ok(val) => match val {
            Some(res) => {
                let mut commits: Vec<(String, i64, i16)> = bincode::deserialize(&res[..]).unwrap();
                commits.sort_unstable_by_key(|&(_, timestamp, _)| timestamp);
                return Some(commits);
            }
            None => return None,
        },
        Err(_) => return None,
    }
}

pub fn get_author_commits_range(
    opts: &env::Options,
    author: String,
    start_index: usize,
    end_index: usize,
) -> Option<Vec<(String, i64, i16, String)>> {
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    match db.get(author) {
        Ok(val) => match val {
            Some(res) => {
                let mut commits: Vec<(String, i64, i16, String)> = bincode::deserialize::<env::Revisions>(&res[..]).unwrap().revisions;
                commits.sort_unstable_by_key(|&(_, timestamp, _, _)| timestamp);
                return Some(
                    commits
                        .into_iter()
                        .skip(start_index)
                        .take(end_index - start_index + 1)
                        .collect(),
                );
            }
            None => return None,
        },
        Err(_) => return None,
    }
}

fn generate_commits(amount_of_window: usize) -> Vec<env::Window> {
    let mut commits = Vec::new();
    for _ in 0..amount_of_window {
        let size = rand::random_range(3..=10);
        let mut random_vec: Vec<i16> = vec![];
        for _ in 0..size {
            random_vec.push(rand::random_range(-1000..=1000));
        }
        let i: usize = rand::random_range(0..100);
        let j: usize = rand::random_range(i + 1..i + 100);
        let i_date = rand::random_range(946684800..Utc::now().timestamp());
        let j_date = rand::random_range(i_date..i_date + 100_000);
        commits.push(env::Window {
            start_index: i,
            start_time: DateTime::from_timestamp(i_date, 0).unwrap(),
            end_index: j,
            end_time: DateTime::from_timestamp(j_date, 0).unwrap(),
            timezones: random_vec,
            // status: generate_status(),
        });
    }
    commits
}

fn generate_author() -> String {
    const HEX_CHARS: &[u8] = b"0123456789abcdef";
    (0..64)
        .map(|_| {
            let idx = rand::random_range(0..HEX_CHARS.len());
            HEX_CHARS[idx] as char
        })
        .collect()
}

fn _generate_status() -> env::Status {
    let random_value = rand::random_range(0..=2);
    match random_value {
        0 => env::Status::Author,
        1 => env::Status::Committer,
        2 => env::Status::Both,
        _ => env::Status::Unkmown,
    }
}

pub fn generate_test_sample(amount_of_author: usize) -> Vec<env::Windows> {
    let mut test = Vec::new();
    for _ in 0..amount_of_author {
        let amount_of_window = rand::random_range(1..=100);
        test.push(env::Windows {
            author: generate_author(),
            metadata: env::Metadata::default(),
            windows: generate_commits(amount_of_window),
        });
    }
    test
}

pub fn windows_len(res: &Vec<env::Windows>) -> usize {
    let mut size = 0;
    res.iter().for_each(|windows| {
        size += windows.windows.len();
    });
    size
}

pub fn flights_len(res: &Vec<env::Flights>) -> usize {
    let mut size = 0;
    res.iter().for_each(|flights| {
        size += flights.flights.len();
    });
    size
}
