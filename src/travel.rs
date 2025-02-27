use std::fs::{self};

use chrono::{DateTime, Utc};
use log::{error, warn};
use serde::Serialize;

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

    if let Ok(_) = fs::metadata(&filename) {
        warn!("travel.rs > flush_res | File {} already exists!", &filename);
        return;
    }
    let mut writer = csv::Writer::from_path(&filename).unwrap();
    // writer.write_record(&["author", "name", "email", "start_index", "start_time", "end_index", "end_time", "timezones"]).unwrap();

    #[derive(Serialize)]
    struct Field {
        author: String,
        name: String,
        email: String,
        commit_a: String,
        commit_b: String,
        timestamp_a: DateTime<Utc>,
        timestamp_b: DateTime<Utc>,
        timezones: (i16, i16),
    }

    res.iter().for_each(|flights| {
        flights.flights.iter().for_each(|flight| {
            writer
                .serialize(Field {
                    author: flights.author.clone(),
                    name: flights.metadata.name.clone(),
                    email: flights.metadata.email.clone(),
                    commit_a: flight.commits.0.to_owned(),
                    commit_b: flight.commits.1.to_owned(),
                    timestamp_a: flight.timestamps.0,
                    timestamp_b: flight.timestamps.1,
                    timezones: flight.timezones,
                })
                .unwrap();
        });
    });
    writer.flush().unwrap();
    res.clear();
}
