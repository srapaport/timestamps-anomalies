pub mod env;
mod retrieval;
mod trivial_analysis;
use std::time::Instant;
use std::collections::{HashSet, HashMap};
use serde::Serialize;

use rocksdb::{DB, Options};
use csv;

use log::{info, warn};

pub fn retrieve_revision_timezones(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_db(retrieval::load_revisions_from_orc(&opts.orc_dir), &opts);
    info!("Total retrieval time: {:.2?}", start.elapsed());
}

pub fn filter_timezone(opts: &env::Options){
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    //let mut res: HashMap<i64, HashMap<String, HashSet<(String, String)>>> = HashMap::new();
    let mut csv_wrt = csv::WriterBuilder::new().delimiter(b';').from_path("./results/2021/all_ts_2021.csv").unwrap();
    let mut amount_of_err = 0;
    for result in db.iterator(rocksdb::IteratorMode::Start) {
        match result {
            Ok((key, value)) => {
                let author = String::from_utf8(key.to_vec()).unwrap();
                let dates: HashSet<(String, i64, i16)> = bincode::deserialize(&value[..]).unwrap();
                if trivial_analysis::more_than_one_timezone(&dates){
                    dates.into_iter().for_each(|date|{
                        csv_wrt.serialize(env::AuthorCommit{
                            author: author.clone(),
                            commit_id: date.0,
                            timestamp_utc0: (date.1 - ((date.2 as i64) * 60)),
                            timezone: date.2,
                        }).unwrap();
                    });
                }
            },
            Err(_) => {
                amount_of_err += 1;
                warn!("data unreadable from rocksdb {amount_of_err}");
                continue;
            },
        }
    }
    csv_wrt.flush().unwrap();
}


