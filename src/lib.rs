pub mod env;
mod retrieval;
pub mod utils;
mod window;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::ThreadPoolBuilder;
use rocksdb::{Options, DB};
use utils::windows_len;
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

pub fn retrieve_revision_timezones_author_committer(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_db(
        retrieval::load_revisions_from_orc_same_author_committer(&opts),
        &opts,
    );
    info!("Total retrieval time: {:.2?}", start.elapsed());
}

pub fn window_tz_inconsistency(opts: &env::Options) {
    let current_cp: Vec<String>;
    let results_path = opts.results.clone();
    match window::load_checkpoint(&results_path) {
        Some(cp) => current_cp = cp,
        None => current_cp = Vec::new(),
    }
    let (tx, rx) = channel::<Option<(String, Vec<env::Window>)>>();
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
                            let commits: Vec<(String, i64, i16)> =
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
                                            commits,
                                            window_range,
                                            // &multi_bar,
                                        );
                                    if suspicious_pattern.len() > 0 {
                                        amount_window_tz_inconsistency
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    tx.send(Some((author.clone(), suspicious_pattern))).expect(
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
                if windows_len(&res) > opts.chunk {
                    window::flush_res(&opts.results, &mut res);
                    window::create_cp(&opts.results, &mut cp);
                }
                cp.push(windows.0.clone());
                if windows.1.len() > 0 {
                    res.push(env::Windows {
                        author: windows.0,
                        windows: windows.1,
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
        window::create_cp(&opts.results, &mut cp);
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
