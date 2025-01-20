use chrono::{DateTime, Utc};
// use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashSet;

fn remove_consecutive_duplicates(sequence: Vec<i16>) -> Vec<i16> {
    if sequence.is_empty() {
        return vec![];
    }
    let mut result = vec![sequence[0].clone()];
    for item in sequence.iter().skip(1) {
        if *item != *result.last().unwrap() {
            result.push(item.clone());
        }
    }
    result
}

pub fn window_tz_inconsistency_extraction(
    commits: HashSet<(String, i64, i16)>,
    window_range: i64,
    // progress: &MultiProgress,
) -> Vec<Vec<(String, DateTime<Utc>, i16)>> {
    //sort commits
    let mut sorted_commits: Vec<_> = commits.into_iter().collect();
    sorted_commits.sort_by_key(|&(_, timestamp, _)| timestamp);
    let sorted_commits: Vec<(String, DateTime<Utc>, i16)> = sorted_commits.into_iter().map(|commit|{
        (commit.0, DateTime::from_timestamp(commit.1, 0).unwrap(), commit.2)
    }).collect();

    let mut suspicious_pattern: Vec<Vec<(String, DateTime<Utc>, i16)>> = Vec::new();
    let mut last_commit: Option<(String, DateTime<Utc>, i16)> = None;

    // let bar = progress.add(ProgressBar::new(sorted_commits.len() as u64));
    // bar.set_style(
    //     ProgressStyle::with_template(
    //         "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
    //     )
    //     .unwrap(),
    // );
    for i in 0..sorted_commits.len() {
        let mut current_window = vec![sorted_commits[i].clone()];

        for j in i + 1..sorted_commits.len() {
            let time_diff = sorted_commits[j].1 - current_window[0].1;
            if time_diff.num_seconds() > window_range {
                break;
            }
            current_window.push(sorted_commits[j].clone());
        }

        if current_window.len() > 2
            && last_commit.as_ref() != Some(&current_window.last().unwrap().clone())
        {
            let tzs: Vec<i16> = current_window.iter().map(|commit| commit.2).collect();

            if remove_consecutive_duplicates(tzs).len() > 2 {
                suspicious_pattern.push(current_window.clone());
            }
        }

        last_commit = Some(current_window.last().unwrap().clone());
        //bar.inc(1);
    }
    // bar.finish_and_clear();
    // progress.remove(&bar);
    suspicious_pattern
}
