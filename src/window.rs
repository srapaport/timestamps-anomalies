use std::{
    fs::{self},
    time::Duration,
};

use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::{error, warn};
use serde::Serialize;

use crate::env;

///    [
///        ((i, tsi), (j, tsj), [tz1, tz2, ...]),
///        (...)
///    ]
pub fn window_tz_inconsistency_extraction(
    mut commits: Vec<(String, i64, i16, String)>,
    window_range: u64,
) -> Vec<env::Window> {
    commits.sort_unstable_by_key(|&(_, timestamp, _, _)| timestamp);
    let refactored_commits: Vec<(String, DateTime<Utc>, i16, String)> = commits
        .into_iter()
        .filter_map(|commit: (String, i64, i16, String)| {
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
                commit.3,
            ))
        })
        .collect();
    let bar = ProgressBar::new(refactored_commits.len() as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{msg} {wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );
    let mut suspicious_pattern: Vec<env::Window> = Vec::new();
    let mut min_time = None;
    let mut min_offset = 0;
    for (i, first_commit) in refactored_commits.iter().enumerate() {
        if min_time.is_some_and(|time| first_commit.1 < time) {
            bar.inc(1);
            continue;
        }
        if refactored_commits
            .get(i + 1)
            .is_some_and(|(_, _, tz, _)| first_commit.2 == *tz)
        {
            bar.inc(1);
            continue;
        }
        let max_time = first_commit.1 + Duration::from_secs(window_range);
        let count = refactored_commits
            .iter()
            .skip(usize::max(i + 1, min_offset))
            .take_while(|commit| commit.1 <= max_time)
            .count()
            + 1
            + usize::max(min_offset, i + 1)
            - (i + 1);
        if count > 2 {
            let timezone_seq: Vec<i16> = refactored_commits[i..i + count]
                .iter()
                .map(|commit| commit.2)
                .dedup()
                .collect();
            if timezone_seq.iter().count() > 2 {
                //suspicious_pattern.push(commits[i..i + count].to_vec());
                suspicious_pattern.push(env::Window {
                    start_index: i,
                    start_time: refactored_commits[i].1,
                    end_index: i + count - 1,
                    end_time: refactored_commits[i + count - 1].1,
                    timezones: timezone_seq,
                });
            }
        }
        if let Some(next_commit) = refactored_commits.get(i + count) {
            min_time = Some(next_commit.1 - Duration::from_secs(window_range));
            min_offset = i + count;
        } else {
            break;
        }
        bar.inc(1);
    }
    bar.finish_and_clear();
    suspicious_pattern
}

pub fn flush_res(path_res: &str, res: &mut Vec<env::Windows>) {
    let author = res.first().unwrap().author.clone();
    let filename = format!("{}/{}.json", path_res, author);

    if let Ok(_) = fs::metadata(&filename) {
        warn!("window.rs > flush_res | File {} already exists!", &filename);
        return;
    }

    let mut writer = csv::Writer::from_path(&filename).unwrap();
    // writer.write_record(&["author", "name", "email", "start_index", "start_time", "end_index", "end_time", "timezones"]).unwrap();

    #[derive(Serialize)]
    struct Field {
        author: String,
        name: String,
        email: String,
        start_index: usize,
        start_time: DateTime<Utc>,
        end_index: usize,
        end_time: DateTime<Utc>,
        timezones: Vec<i16>,
    }

    res.iter().for_each(|windows| {
        windows.windows.iter().for_each(|window| {
            writer
                .serialize(Field {
                    author: windows.author.clone(),
                    name: windows.metadata.name.clone(),
                    email: windows.metadata.email.clone(),
                    start_index: window.start_index,
                    start_time: window.start_time,
                    end_index: window.end_index,
                    end_time: window.end_time,
                    timezones: window.timezones.clone(),
                })
                .unwrap();
        });
    });
    writer.flush().unwrap();
    res.clear();
}
