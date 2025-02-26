use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use chrono::{DateTime, Utc};
use timestamps_anomalies::env;
use std::{
    fs::File,
    io::Write,
    time::Duration,
};
use itertools::Itertools;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::ThreadPoolBuilder;
use rocksdb::{Options, DB};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use log::{error, warn};


fn main(){
    let stat_commits: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let options = Options::default();
    let db = DB::open(&options, "./rocksdb_timezones_2024").unwrap();
    let bar = ProgressBar::new(env::AUTHOR_DB_2024);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
        )
        .unwrap(),
    );
    let workers = 80;
    let pool = ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .unwrap();
    let amount_author_parsed = AtomicUsize::new(0);
    let amount_of_err = AtomicUsize::new(0);
    pool.install(|| {
        rayon::scope(|thread| {
            for result in db.iterator(rocksdb::IteratorMode::Start) {
                match result {
                    Ok((_, value)) => {
                        let commits: Vec<(String, i64, i16)> =
                            bincode::deserialize(&value[..]).unwrap();

                        thread.spawn({
                            let mut commits = commits.clone();
                            let amount_author_parsed = &amount_author_parsed;
                            let stat_commits = stat_commits.clone();
                            move |_| {
                                window_commit_extraction(&mut commits, stat_commits.clone());
                                travel_time_commit(&mut commits, stat_commits.clone());
                                amount_author_parsed.fetch_add(1, Ordering::Relaxed);
                            }
                        });
                        bar.inc(1);
                    }
                    Err(_) => {
                        amount_of_err.fetch_add(1, Ordering::Relaxed);
                        warn!("data unreadable from rocksdb");
                        continue;
                    }
                }
            }
        });
    });
    let output_path = "list_commits.txt";
    let mut file = File::create(output_path).unwrap();
    let stats = stat_commits.lock().unwrap();
    for commit in  stats.iter() {
        file.write(format!("{}\n", commit.clone()).as_bytes()).expect(format!("couldn't write {} in file", commit.clone()).as_str());
    }

}

fn window_commit_extraction(
    commits: &mut Vec<(String, i64, i16)>,
    stats: Arc<Mutex<HashSet<String>>>
){
    //sort commits
    commits.sort_unstable_by_key(|&(_, timestamp, _)| timestamp);
    let commits: Vec<(String, DateTime<Utc>, i16)> = commits
        .into_iter()
        .filter_map(|commit| {
            Some((
                commit.0.clone(),
                match DateTime::from_timestamp(commit.1, 0){
                    Some(dt)=>dt,
                    None => {
                        error!("couldn't convert the following timestamp in DateTime: {}, from commit {}", commit.1, commit.0);
                        if let Ok(mut guard) = env::ERROR_DATETIME.lock() {
                            guard.push((commit.0.clone(), commit.1));
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
        let max_time = first_commit.1 + Duration::from_secs(env::ONE_WEEK);
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
                stats.lock().unwrap().insert(first_commit.0.clone());
                return;
            }
        }
        if let Some(next_commit) = commits.get(i + count) {
            min_time = Some(next_commit.1 - Duration::from_secs(env::ONE_WEEK));
            min_offset = i + count;
        } else {
            break;
        }
    }
}

fn travel_time_commit(commits: &mut Vec<(String, i64, i16)>, stats: Arc<Mutex<HashSet<String>>>) {
    commits.sort_unstable_by_key(|&(_, timestamp, _)| timestamp);
    for (i, current_commit) in commits.iter().enumerate() {
        if let Some(next_commit) = commits.get(i + 1) {
            if next_commit.2 == current_commit.2 {
                continue;
            }
            let flight_time = (next_commit.2 - current_commit.2).abs() as i64;
            let time_diff = next_commit.1 - current_commit.1;
            if flight_time > time_diff {
                stats.lock().unwrap().insert(current_commit.0.clone());
                return;
            }
        }
    }
}
