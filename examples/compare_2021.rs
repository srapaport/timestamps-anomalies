use chrono::{DateTime, Utc};
use rocksdb::{Options, DB};
use std::time::Instant;

use log::error;
fn main() {
    /* let sample = vec!["8cbcaea89cfc4603c954f50aaddc3172446961d1",
    "c0f17caddcd894f68340ba1c66151cc6b26aa5de",
    "4733aa3b37028029513937da7f46c61393609c50",
    "6ecff27e1d6c1eef20d71a5c1ea34514db628e80",
    "09f02304af6d26b45122a47b48f15619299672f4",
    "78e58ecd7dbda340d15c64ed4dbce1de300105c7",
    "d2153a46f57f9a774e1c4432d19f19b9f775d21d",
    "a07c27d6a6a6f43a1a635caf25309e528c2c60de",
    "26f0740ed6a211bd81edc9118526636e814bd650",
    "186a4f80d30e45501557a4a8081e910e787e7dc3"];
    test("./rocksdb_timezones_2021", sample.clone());
    println!("---------------------------------");
    test("./rocksdb_test_2021", sample); */
    let start = Instant::now();
    let amount_author_rocksDB = DB::open(&Options::default(), "./rocksdb_timezones_2024")
        .unwrap()
        .iterator(rocksdb::IteratorMode::Start)
        .count();
    println!("time elapsed: {:.2?}", start.elapsed());
    println!("amount of author in rocksDB: {}", amount_author_rocksDB);
}

fn test(db_path: &str, sample: Vec<&str>) {
    let options = Options::default();
    let db = DB::open(&options, db_path).unwrap();

    for result in db.iterator(rocksdb::IteratorMode::Start) {
        match result {
            Ok((_, value)) => {
                let commits: Vec<(String, i64, i16)> = bincode::deserialize(&value[..]).unwrap();
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
                for commit in commits {
                    if sample.contains(&commit.0.as_str()) {
                        println!(
                            "id: {:?}, date {:?}, time: {:?}, tz: {}",
                            commit.0,
                            commit.1.date_naive(),
                            commit.1.time(),
                            commit.2
                        );
                        break;
                    }
                }
            }
            Err(err) => {
                error!("data unreadable from rocksdb {err}");
                continue;
            }
        }
    }
}
