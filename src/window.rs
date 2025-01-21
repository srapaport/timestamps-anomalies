use chrono::{DateTime, Utc};
// use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;

pub fn window_tz_inconsistency_extraction(
    mut commits: Vec<(String, i64, i16)>,
    window_range: i64,
    // progress: &MultiProgress,
) -> Vec<Vec<(String, DateTime<Utc>, i16)>> {
    //sort commits

    commits.sort_unstable_by_key(|&(_, timestamp, _)| timestamp);
    let commits: Vec<(String, DateTime<Utc>, i16)> = commits.into_iter().map(|commit|{
        (commit.0, DateTime::from_timestamp(commit.1, 0).unwrap(), commit.2)
    }).collect();

    let mut suspicious_pattern: Vec<Vec<(String, DateTime<Utc>, i16)>> = Vec::new();
    let mut last_commit: Option<&(String, DateTime<Utc>, i16)> = None;

    // let bar = progress.add(ProgressBar::new(commits.len() as u64));
    // bar.set_style(
    //     ProgressStyle::with_template(
    //         "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
    //     )
    //     .unwrap(),
    // );
    for (i, first_commit) in commits.iter().enumerate() {
        if i % 1000 == 0{
            println!("{i}");
        }
        
        let count = commits.iter().skip(i+1).take_while(|commit|{
            (commit.1 - first_commit.1).num_seconds() <= window_range
        }).count() + 1;
        if count > 2{
            println!("count = {count}");
            if Some(&commits[i+count-1]) != last_commit && commits[i..i+count].iter().map(|commit| commit.2).dedup().count() > 2{
                suspicious_pattern.push(commits[i..i+count].to_vec());
            }
            last_commit = Some(&commits[i+count-1]);
        }
            
    }
    //bar.inc(1);
    
    // bar.finish_and_clear();
    // progress.remove(&bar);
    suspicious_pattern
}
