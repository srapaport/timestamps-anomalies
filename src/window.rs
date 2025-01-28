use std::{fs::{self, File}, io::Write, time::Duration};

use chrono::{DateTime, Utc};
// use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::warn;

///    [
///        ((i, tsi), (j, tsj), [tz1, tz2, ...]),
///        (...)
///    ]
pub fn window_tz_inconsistency_extraction(
    mut commits: Vec<(String, i64, i16)>,
    window_range: u64,
    // progress: &MultiProgress,
) -> Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)> {
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

    //let mut suspicious_pattern: Vec<Vec<(String, DateTime<Utc>, i16)>> = Vec::new();
    let mut suspicious_pattern: Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)> =
        Vec::new();

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
            let timezone_seq: Vec<i16> = commits[i..i + count]
                .iter()
                .map(|commit| commit.2)
                .dedup()
                .collect();
            if timezone_seq.iter().count() > 2 {
                //suspicious_pattern.push(commits[i..i + count].to_vec());
                suspicious_pattern.push((
                    (i, commits[i].1),
                    (i + count, commits[i + count].1),
                    timezone_seq,
                ));
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

pub fn flush_res(path_res: &str, res: &mut Vec<(String, Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>)>){
    let author = res.first().unwrap().0.clone();
    let filename = format!("{}/{}.res", path_res, author);
    let filename_tmp = format!("{}/{}.tmp", path_res, author);
    if let Ok(_) = fs::metadata(&filename_tmp){
        warn!("window.rs > flush_res | File {} already exists!", &filename_tmp);
        return;
    }
    if let Ok(_) = fs::metadata(&filename){
        warn!("window.rs > flush_res | File {} already exists!", &filename);
        return;
    }
    let Ok(mut file_w) = File::create_new(format!("{}", filename_tmp)) else {
        warn!("File {} already exists!", filename_tmp);
        return;
    };
    res.iter().for_each(|(author, commits)|{

        file_w
            .write(format!("{}:[ ", author).as_bytes())
            .expect(format!("couldn't write datas in file: {}", filename_tmp).as_str());
        commits.iter().for_each(|commit|{
            file_w.write(format!("({}, {} -> {}, {} tzs: {:?}) ", commit.0.0, commit.0.1.timestamp(), commit.1.0, commit.1.1.timestamp(), commit.2).as_bytes())
                .expect(format!("couldn't write datas in file: {}", filename_tmp).as_str());
        });
        file_w
            .write("]\n".as_bytes())
            .expect(format!("couldn't write datas in file: {}", filename_tmp).as_str());
    });
    res.clear();
    std::fs::rename(&filename_tmp, &filename)
        .expect(format!("Couldn't rename {} into {}", filename_tmp, filename).as_str());
}

pub fn create_cp(path_res: &str, cp: &mut Vec<String>){

    cp.clear();
}

// creates a new checkpoint in the file checkpoints
/* fn create_cp(prefix: &str, cp: &mut HashSet<String>) {
    let filename = format!("{}/checkpoints", prefix);
    if let Err(_) = fs::metadata(&filename) {
        File::create_new(&filename)
            .expect(format!("lib.rs > create_cp | File {} already exists!", &filename).as_str());
    }
    let mut cp_file = fs::OpenOptions::new()
        .append(true)
        .open(filename)
        .expect(format!("cannot open checkpoints file").as_str());
    cp.iter().for_each(|ori| {
        cp_file
            .write(format!("{}\n", ori).as_bytes())
            .expect("couldn't write data in checkpoints");
    });
    cp.clear();
} */