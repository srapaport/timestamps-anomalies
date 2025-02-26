pub mod env;
mod retrieval;
mod travel;
pub mod utils;
mod window;
use chashmap::CHashMap;
use csv::{ReaderBuilder, StringRecord};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rocksdb::{Options, DB};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Instant;
use utils::flights_len;
use utils::windows_len;

use log::{info, warn};

pub fn retrieve_revision_timezones(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_db(retrieval::load_revisions_from_orc(&opts), &opts);
    info!("Total retrieval time: {:.2?}", start.elapsed());
}

/* pub fn retrieve_revision_timezones_author_committer(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_db(
        retrieval::load_revisions_from_orc_same_author_committer(&opts),
        &opts,
    );
    info!("Total retrieval time: {:.2?}", start.elapsed());
} */

pub fn retrieve_revision_through_time(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_revision_year(retrieval::load_revisions_from_orc_through_time(opts), opts);
    info!("Total retrieval time: {:.2?}", start.elapsed());
}

pub fn load_authors(path: &str) -> CHashMap<String, env::CommitAuthor> {
    let authors = CHashMap::with_capacity(2_411_751);
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(path)
        .unwrap();
    let records = rdr
        .records()
        .collect::<Result<Vec<StringRecord>, csv::Error>>()
        .unwrap();
    records.into_par_iter().for_each(|record| {
    // records.into_iter().for_each(|record| {
        let mut commit_author = env::CommitAuthor::default();
        let mut commit_id = String::new();
        let mut field_id = 0;
        record.iter().for_each(|field| {
            match field_id {
                0 => {
                    commit_id = String::from(field);
                    field_id += 1;
                }
                1 | 2 => {
                    let cleaned = field.strip_prefix("b").unwrap_or(field).trim_matches('\'');
                    if let Some(email_start) = cleaned.rfind('<') {
                        let name = cleaned[..email_start].trim();
                        let email_end;
                        match cleaned[email_start..].find('>') {
                            Some(index) => email_end = email_start + index,
                            None => email_end = cleaned.chars().count() - 1,
                        }
                        let email = cleaned[email_start + 1..email_end].trim();
                        if field_id == 1 {
                            commit_author.author_name = String::from(name);
                            commit_author.author_email = String::from(email);
                        } else {
                            commit_author.committer_name = String::from(name);
                            commit_author.committer_email = String::from(email);
                        }
                    } else {
                        // No difference name / email
                        if field_id == 1 {
                            commit_author.author_name = String::from(cleaned);
                            commit_author.author_email = String::from(cleaned);
                        } else {
                            commit_author.committer_name = String::from(cleaned);
                            commit_author.committer_email = String::from(cleaned);
                        }
                    }
                    field_id += 1;
                }
                _ => (),
            }
        });
        authors.insert(commit_id, commit_author);
    });
    authors
}

pub fn window_tz_inconsistency(opts: &env::Options) {
    let current_cp: Vec<String>;
    let results_path = opts.results.clone();
    match utils::load_checkpoint(&results_path) {
        Some(cp) => current_cp = cp,
        None => current_cp = Vec::new(),
    }
    let (tx, rx) = channel::<Option<(String, env::Metadata, Vec<env::Window>)>>();
    let path_db = opts.output_dir.clone();
    let amount_author_db = opts.amount_authors;
    let window_range = opts.window_range;
    let jh = thread::spawn(move || {
        let current_cp = &current_cp;
        //open rocksdb
        let options = Options::default();
        let db = DB::open(&options, &path_db).unwrap();
        let amount_of_err = AtomicUsize::new(0);
        let amount_window_tz_inconsistency = AtomicUsize::new(0);
        let amount_author_parsed = AtomicUsize::new(0);
        // let mut author_window_tz_inconsistency = vec![];
        let multi_bar = MultiProgress::new();
        let bar = multi_bar.add(ProgressBar::new(amount_author_db));
        bar.set_style(
            ProgressStyle::with_template(
                "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
            )
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
                            if current_cp.contains(&author) {
                                bar.inc(1);
                                continue;
                            }
                            let commits: env::Revisions =
                                bincode::deserialize(&value[..]).unwrap();
                            //call window_tz_inconsistency_extraction for each author
                            // bar.set_message(format!("checking inconsistency for author {:?}", author));
                            thread.spawn({
                                let commits = commits.clone();
                                let amount_window_tz_inconsistency =
                                    &amount_window_tz_inconsistency;
                                let amount_author_parsed = &amount_author_parsed;
                                let tx = tx.clone();
                                move |_| {
                                    let suspicious_pattern =
                                        window::window_tz_inconsistency_extraction(
                                            commits.revisions,
                                            window_range,
                                            // &multi_bar,
                                        );
                                    if suspicious_pattern.len() > 0 {
                                        amount_window_tz_inconsistency
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    tx.send(Some((author.clone(), commits.metadata.clone(), suspicious_pattern))).expect(
                                        format!(
                                            "Failed sending the message for author: {}",
                                            author
                                        )
                                        .as_str(),
                                    );
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
        bar.finish_and_clear();
        write_error_time(results_path).expect("couldn't flush error messages");
        println!(
            "Amount of author parsed: {}",
            amount_author_parsed.load(Ordering::Relaxed)
        );
        println!(
            "Amount of author flagged: {}",
            amount_window_tz_inconsistency.load(Ordering::Relaxed)
        );
        println!(
            "Ratio of author changing timezones at least 2 times in a given window: {:.2}%",
            (amount_window_tz_inconsistency.load(Ordering::Relaxed) as f64
                / amount_author_db as f64)
                * 100.0
        );
        println!(
            "Amount of RocksDB err: {}",
            amount_of_err.load(Ordering::Relaxed)
        );
        println!(
            "Amount of datetime err: {}",
            env::ERROR_DATETIME.lock().unwrap().len()
        );
    });

    let mut res: Vec<env::Windows> = Vec::new();
    let mut cp: Vec<String> = Vec::new();
    let mut recv_err = AtomicUsize::new(0);
    for _ in 0..opts.amount_authors {
        if let Ok(package) = rx.recv() {
            if let Some(windows) = package {
                if (res.len() > 0) && ((windows_len(&res) + windows.2.len()) > opts.chunk) {
                    window::flush_res(&opts.results, &mut res);
                    utils::create_cp(&opts.results, &mut cp);
                }
                cp.push(windows.0.clone());
                if windows.2.len() > 0 {
                    res.push(env::Windows {
                        author: windows.0,
                        metadata: windows.1,
                        windows: windows.2,
                    });
                }
            }
        } else {
            warn!("Couldn't receive package");
            recv_err.fetch_add(1, Ordering::Relaxed);
        }
    }
    if res.len() > 0 {
        window::flush_res(&opts.results, &mut res);
    }
    if cp.len() > 0 {
        utils::create_cp(&opts.results, &mut cp);
    }
    println!("Amount of package not received: {}", recv_err.get_mut());

    jh.join().unwrap();
}

pub fn flight_inconsistency(opts: &env::Options) {
    let current_cp: Vec<String>;
    let results_path = opts.results.clone();
    match utils::load_checkpoint(&results_path) {
        Some(cp) => current_cp = cp,
        None => current_cp = Vec::new(),
    }
    let (tx, rx) = channel::<Option<(String, env::Metadata, Vec<env::Flight>)>>();
    let path_db = opts.output_dir.clone();
    let amount_author_db = opts.amount_authors;
    let jh = thread::spawn(move || {
        let current_cp = &current_cp;
        //open rocksdb
        let options = Options::default();
        let db = DB::open(&options, &path_db).unwrap();
        let amount_of_err = AtomicUsize::new(0);
        let amount_flight_inconsistency = AtomicUsize::new(0);
        let amount_author_parsed = AtomicUsize::new(0);
        // let mut author_window_tz_inconsistency = vec![];
        let multi_bar = MultiProgress::new();
        let bar = multi_bar.add(ProgressBar::new(amount_author_db));
        bar.set_style(
            ProgressStyle::with_template(
                "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
            )
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
                            if current_cp.contains(&author) {
                                bar.inc(1);
                                continue;
                            }
                            let commits: env::Revisions =
                                bincode::deserialize(&value[..]).unwrap();
                            //call window_tz_inconsistency_extraction for each author
                            // bar.set_message(format!("checking inconsistency for author {:?}", author));
                            thread.spawn({
                                let commits = commits.clone();
                                let amount_flight_inconsistency = &amount_flight_inconsistency;
                                let amount_author_parsed = &amount_author_parsed;
                                let tx = tx.clone();
                                move |_| {
                                    let suspicious_pattern = travel::travel_time(commits.revisions);
                                    if suspicious_pattern.len() > 0 {
                                        amount_flight_inconsistency.fetch_add(1, Ordering::Relaxed);
                                    }
                                    tx.send(Some((author.clone(), commits.metadata.clone(), suspicious_pattern))).expect(
                                        format!(
                                            "Failed sending the message for author: {}",
                                            author
                                        )
                                        .as_str(),
                                    );
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
        bar.finish_and_clear();
        write_error_time(results_path).expect("couldn't flush error messages");
        println!(
            "Amount of author parsed: {}",
            amount_author_parsed.load(Ordering::Relaxed)
        );
        println!(
            "Amount of author flagged: {}",
            amount_flight_inconsistency.load(Ordering::Relaxed)
        );
        println!(
            "Ratio of author changing timezones quickly: {:.2}%",
            (amount_flight_inconsistency.load(Ordering::Relaxed) as f64 / amount_author_db as f64)
                * 100.0
        );
        println!(
            "Amount of RocksDB err: {}",
            amount_of_err.load(Ordering::Relaxed)
        );
        println!(
            "Amount of datetime err: {}",
            env::ERROR_DATETIME.lock().unwrap().len()
        );
    });

    let mut res: Vec<env::Flights> = Vec::new();
    let mut cp: Vec<String> = Vec::new();
    let mut recv_err = AtomicUsize::new(0);
    for _ in 0..opts.amount_authors {
        if let Ok(package) = rx.recv() {
            if let Some(flights) = package {
                if (res.len() > 0) && ((flights_len(&res) + flights.2.len()) > opts.chunk) {
                    travel::flush_res(&opts.results, &mut res);
                    utils::create_cp(&opts.results, &mut cp);
                }
                cp.push(flights.0.clone());
                if flights.2.len() > 0 {
                    res.push(env::Flights {
                        author: flights.0,
                        metadata: flights.1,
                        flights: flights.2,
                    });
                }
            }
        } else {
            warn!("Couldn't receive package");
            recv_err.fetch_add(1, Ordering::Relaxed);
        }
    }
    if res.len() > 0 {
        travel::flush_res(&opts.results, &mut res);
    }
    if cp.len() > 0 {
        utils::create_cp(&opts.results, &mut cp);
    }
    println!("Amount of package not received: {}", recv_err.get_mut());

    jh.join().unwrap();
}

pub fn write_error_time(results: String) -> std::io::Result<()> {
    let error_times = env::ERROR_DATETIME.lock().unwrap();
    let path = format!("{}/logs/error_time.json", results);
    std::fs::write(path, serde_json::to_string(&*error_times).unwrap())?;
    Ok(())
}
