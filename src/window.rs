use std::time::Duration;

use chrono::{DateTime, Utc};
// use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;

pub fn window_tz_inconsistency_extraction(
    mut commits: Vec<(String, i64, i16)>,
    window_range: u64,
    // progress: &MultiProgress,
) -> Vec<Vec<(String, DateTime<Utc>, i16)>> {
    //sort commits

    commits.sort_unstable_by_key(|&(_, timestamp, _)| timestamp);
    let commits: Vec<(String, DateTime<Utc>, i16)> = commits
        .into_iter()
        .map(|commit| {
            (
                commit.0,
                DateTime::from_timestamp(commit.1, 0).unwrap(),
                commit.2,
            )
        })
        .collect();

    let mut suspicious_pattern: Vec<Vec<(String, DateTime<Utc>, i16)>> = Vec::new();

    // let bar = progress.add(ProgressBar::new(commits.len() as u64));
    // bar.set_style(
    //     ProgressStyle::with_template(
    //         "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
    //     )
    //     .unwrap(),
    // );
    let mut min_time = None;
    let mut min_offset = 0;
    for (i, first_commit) in commits.iter().enumerate() {
        if min_time.is_some_and(|time| first_commit.1 < time) {
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
            if commits[i..i + count]
                .iter()
                .map(|commit| commit.2)
                .dedup()
                .count()
                > 2
            {
                suspicious_pattern.push(commits[i..i + count].to_vec());
            }
        }
        if let Some(next_commit) = commits.get(i + count) {
            min_time = Some(next_commit.1 - Duration::from_secs(window_range));
            min_offset = i + count;
        } else {
            break;
        }
    }
    //bar.inc(1);

    // bar.finish_and_clear();
    // progress.remove(&bar);
    suspicious_pattern
}
