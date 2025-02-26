use std::{
    fs::{self, File},
    io::Write,
};

use chrono::DateTime;
use log::{error, warn};

use crate::env;

pub fn travel_time(mut commits: Vec<(String, i64, i16, String)>) -> Vec<env::Flight> {
    commits.sort_unstable_by_key(|&(_, timestamp, _, _)| timestamp);
    let mut res = Vec::new();
    for (i, current_commit) in commits.iter().enumerate() {
        if let Some(next_commit) = commits.get(i + 1) {
            if next_commit.2 == current_commit.2 {
                continue;
            }
            let flight_time = (next_commit.2 - current_commit.2).abs() as i64;
            let time_diff = next_commit.1 - current_commit.1;
            if flight_time > time_diff {
                let Some(current_date) = DateTime::from_timestamp(current_commit.1, 0) else {
                    if let Ok(mut guard) = env::ERROR_DATETIME.lock() {
                        guard.push((current_commit.0.clone(), current_commit.1));
                    } else {
                        error!("Failed to acquire lock for ERROR_DATETIME");
                    }
                    continue;
                };
                let Some(next_date) = DateTime::from_timestamp(next_commit.1, 0) else {
                    if let Ok(mut guard) = env::ERROR_DATETIME.lock() {
                        guard.push((next_commit.0.clone(), next_commit.1));
                    } else {
                        error!("Failed to acquire lock for ERROR_DATETIME");
                    }
                    continue;
                };
                res.push(env::Flight {
                    commits: (current_commit.0.clone(), next_commit.0.clone()),
                    timestamps: (current_date, next_date),
                    timezones: (current_commit.2, next_commit.2),
                });
            }
        }
    }
    res
}

pub fn flush_res(path_res: &str, res: &mut Vec<env::Flights>) {
    let author = res.first().unwrap().author.clone();
    let filename = format!("{}/{}.json", path_res, author);
    let filename_tmp = format!("{}/{}.tmp", path_res, author);
    if let Ok(_) = fs::metadata(&filename_tmp) {
        warn!(
            "travel.rs > flush_res | File {} already exists!",
            &filename_tmp
        );
        return;
    }
    if let Ok(_) = fs::metadata(&filename) {
        warn!("travel.rs > flush_res | File {} already exists!", &filename);
        return;
    }
    let Ok(mut file_w) = File::create_new(format!("{}", filename_tmp)) else {
        warn!("File {} already exists!", filename_tmp);
        return;
    };

    let json = serde_json::to_string(res).expect("couldn't serialize windows");
    file_w
        .write(json.as_bytes())
        .expect(format!("couldn't write datas in file: {}", filename_tmp).as_str());

    res.clear();
    std::fs::rename(&filename_tmp, &filename)
        .expect(format!("Couldn't rename {} into {}", filename_tmp, filename).as_str());
}
