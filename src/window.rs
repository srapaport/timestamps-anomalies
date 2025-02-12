use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    time::Duration,
};

use chrono::{DateTime, Utc};
// use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::{error, info, warn};

use crate::env;

///    [
///        ((i, tsi), (j, tsj), [tz1, tz2, ...]),
///        (...)
///    ]
pub fn window_tz_inconsistency_extraction(
    mut commits: Vec<(String, i64, i16)>,
    window_range: u64,
) -> Vec<env::Window> {
    //sort commits
    commits.sort_unstable_by_key(|&(_, timestamp, _)| timestamp);
    let commits: Vec<(String, DateTime<Utc>, i16)> = commits
        .into_iter()
        .filter_map(|commit: (String, i64, i16)| {
            Some((
                commit.0.clone(),
                match DateTime::from_timestamp(commit.1, 0){
                    Some(dt)=>dt,
                    None => {
                        error!("couldn't convert the following timestamp in DateTime: {}, from commit {}", commit.1, commit.0);
                        if let Ok(mut guard) = env::ERROR_DATETIME.lock() {
                            guard.push((commit.0, commit.1));
                        } else {
                            error!("Failed to acquire lock for ERROR_DATETIME");
                        }
                        return None;
                    },
                },
                commit.2,
            ))
        })
        .collect();

    let mut suspicious_pattern: Vec<env::Window> = Vec::new();
    let mut min_time = None;
    let mut min_offset = 0;
    for (i, first_commit) in commits.iter().enumerate() {
        if min_time.is_some_and(|time| first_commit.1 < time) {
            continue;
        }
        if commits
            .get(i + 1)
            .is_some_and(|(_, _, tz)| first_commit.2 == *tz)
        {
            continue;
        }
        let max_time = first_commit.1 + Duration::from_secs(window_range);
        let count = commits
            .iter()
            .skip(usize::max(i + 1, min_offset))
            .take_while(|commit| commit.1 <= max_time)
            .count()
            + 1
            + usize::max(min_offset, i + 1)
            - (i + 1);
        if count > 2 {
            let timezone_seq: Vec<i16> = commits[i..i + count]
                .iter()
                .map(|commit| commit.2)
                .dedup()
                .collect();
            if timezone_seq.iter().count() > 2 {
                //suspicious_pattern.push(commits[i..i + count].to_vec());
                suspicious_pattern.push(env::Window {
                    start_index: i,
                    start_time: commits[i].1,
                    end_index: i + count,
                    end_time: commits[i + count - 1].1,
                    timezones: timezone_seq,
                });
            }
        }
        if let Some(next_commit) = commits.get(i + count) {
            min_time = Some(next_commit.1 - Duration::from_secs(window_range));
            min_offset = i + count;
        } else {
            break;
        }
    }
    suspicious_pattern
}

pub fn flush_res(path_res: &str, res: &mut Vec<env::Windows>) {
    let author = res.first().unwrap().author.clone();
    let filename = format!("{}/{}.json", path_res, author);
    let filename_tmp = format!("{}/{}.tmp", path_res, author);
    if let Ok(_) = fs::metadata(&filename_tmp) {
        warn!(
            "window.rs > flush_res | File {} already exists!",
            &filename_tmp
        );
        return;
    }
    if let Ok(_) = fs::metadata(&filename) {
        warn!("window.rs > flush_res | File {} already exists!", &filename);
        return;
    }
    let Ok(mut file_w) = File::create_new(format!("{}", filename_tmp)) else {
        warn!("File {} already exists!", filename_tmp);
        return;
    };
    /* res.iter().for_each(|elt| {
        let json = serde_json::to_string_pretty(elt).expect(&format!(
            "couldn't serialize window for author {}",
            elt.author
        ));
        file_w
            .write(format!("{}\n", json).as_bytes())
            .expect(format!("couldn't write datas in file: {}", filename_tmp).as_str());
    }); */

    let json = serde_json::to_string(res).expect(
        "couldn't serialize windows"
    );
    file_w
        .write(json.as_bytes())
        .expect(format!("couldn't write datas in file: {}", filename_tmp).as_str());

    res.clear();
    std::fs::rename(&filename_tmp, &filename)
        .expect(format!("Couldn't rename {} into {}", filename_tmp, filename).as_str());
}

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
