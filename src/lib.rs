pub mod env;
mod retrieval;
pub mod utils;
mod window;
use chrono::{DateTime, Utc};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::ThreadPoolBuilder;
use rocksdb::{Options, DB};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Instant;

use log::{info, warn};

pub fn retrieve_revision_timezones(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_db(retrieval::load_revisions_from_orc(&opts), &opts);
    info!("Total retrieval time: {:.2?}", start.elapsed());
}

pub fn window_tz_inconsistency(opts: &env::Options) {

    let (tx, rx) = channel::<
        Option<(
            String,
            Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>,
        )>,
    >();
    let path_db = opts.output_dir.clone();
    let jh = thread::spawn(move||{
        //open rocksdb
        let options = Options::default();
        let db = DB::open(&options, &path_db).unwrap();
        let mut amount_of_err = 0;
        let mut amount_window_tz_inconsistency = AtomicUsize::new(0);
        // let mut author_window_tz_inconsistency = vec![];
        let multi_bar = MultiProgress::new();
        let bar = multi_bar.add(ProgressBar::new(env::AMOUNT_AUHTOR_2021));
        bar.set_style(
            ProgressStyle::with_template("{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}")
                .unwrap(),
        );
        let workers = (num_cpus::get() / 3) * 2;
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap();

        pool.install(|| {
            rayon::scope(|thread| {
                for result in db.iterator(rocksdb::IteratorMode::Start) {
                    match result {
                        Ok((key, value)) => {
                            let author = String::from_utf8_lossy(&key).to_string();
                            let commits: Vec<(String, i64, i16)> =
                                bincode::deserialize(&value[..]).unwrap();
                            //call window_tz_inconsistency_extraction for each author
                            // bar.set_message(format!("checking inconsistency for author {:?}", author));
                            thread.spawn({
                                let commits = commits.clone();
                                let amount_window_tz_inconsistency = &amount_window_tz_inconsistency;
                                let tx = tx.clone();
                                move |_| {
                                    let suspicious_pattern = window::window_tz_inconsistency_extraction(
                                        commits,
                                        env::ONE_DAY,
                                        // &multi_bar,
                                    );
                                    if suspicious_pattern.len() > 0 {
                                        amount_window_tz_inconsistency.fetch_add(1, Ordering::Relaxed);
                                    }
                                    tx.send(Some((author.clone(), suspicious_pattern))).expect(
                                        format!(
                                            "Failed sending the message for author: {}",
                                            author
                                        )
                                        .as_str(),
                                    );
                                }
                            });
                            bar.inc(1);
                        }
                        Err(_) => {
                            amount_of_err += 1;
                            warn!("data unreadable from rocksdb {amount_of_err}");
                            continue;
                        }
                    }
                }
            });
        });
        println!(
            "Amount of author flagged: {}",
            amount_window_tz_inconsistency.get_mut()
        );
        println!(
            "Ratio of author changing timezones at least 2 times in a given window: {:.2}%",
            (*amount_window_tz_inconsistency.get_mut() as f64 / env::AMOUNT_AUHTOR_2021 as f64) * 100.0
        );
        println!("Amount of err: {}", amount_of_err);
        bar.finish_with_message("Finish checking all authors");
    });
    
    let mut res: Vec<(String, Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>)> = Vec::new(); 
    let mut cp: Vec<String> = Vec::new();
    for _ in 0..env::AUTHOR_DB_2024{
        if let Some(package) = rx.recv().expect("Couldn't receive package"){
            if res.len() > opts.chunk {
                window::flush_res(&opts.output_dir, &mut res);
                window::create_cp(&opts.output_dir, &mut cp);
            }
            cp.push(package.0.clone());
            if package.1.len() > 0{
                res.push(package);
            }
        } 
    }
    if res.len() > 0 {
        window::flush_res(&opts.output_dir, &mut res);
    }
    if cp.len() > 0 {
        window::create_cp(&opts.output_dir, &mut cp);
    }

    jh.join().unwrap();
}
