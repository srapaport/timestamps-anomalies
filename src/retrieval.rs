use crate::{env, load_authors};
use ar_row::deserialize::ArRowDeserialize;
use chashmap::CHashMap;
use chrono::{DateTime, Datelike};
use indicatif::{ProgressBar, ProgressStyle};
use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;
use rayon::prelude::*;
use serde::Serialize;
use std::fs::{self, File};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use log::{debug, error, info};
// use tracing

pub fn load_revisions_from_orc(opts: &env::Options) -> env::AllRevisions {
    let start = Instant::now();

    let all_revisions = env::AllRevisions::default();

    let files = std::fs::read_dir(&opts.orc_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let amount_rev = AtomicU64::new(0);
    let amount_rev_retrieved = AtomicU64::new(0);
    let amount_none_date = AtomicUsize::new(0);
    let amount_none_offset = AtomicUsize::new(0);
    let amount_none_committer_date = AtomicUsize::new(0);
    let amount_none_committer_offset = AtomicUsize::new(0);
    let amount_overflow_date = AtomicUsize::new(0);
    let amount_bot = AtomicUsize::new(0);
    let bar = ProgressBar::new(opts.amount_revisions as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );
     
    let real_names = load_authors("list_revisions_full.csv");

    files.into_par_iter().for_each(|dir_entry| {
        let orc_path = dir_entry.path();
        info!(
            "orc_path : {}",
            orc_path
                .file_name()
                .unwrap()
                .to_os_string()
                .into_string()
                .unwrap()
        );
        debug!("Extension orc OK");
        let file = File::open(dir_entry.path())
            .expect(format!("could not open .orc {:?}", orc_path).as_str());
        let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
        let projection = ProjectionMask::named_roots(
            builder.file_metadata().root_data_type(),
            &[
                "id",
                "message",
                "author",
                "date",
                "date_offset",
                "committer",
                "committer_date",
                "committer_offset",
                "directory",
            ],
        );
        let reader_builder = builder
            .with_projection(projection.clone())
            .with_batch_size(env::ORC_BATCH_SIZE);
        let schema = env::transform_schema(&reader_builder.schema());
        let reader = reader_builder.with_schema(schema).build();
        reader.for_each(|batch| {
            debug!("Entering reader");
            let batch = batch.unwrap();
            for revision in <env::Revision>::from_record_batch(batch).unwrap() {
                let current_id = revision.id;
                let current_author: String = revision
                    .author
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                let current_date = revision.date;
                let current_date_offset = revision.date_offset;
                let current_committer: String = revision
                    .committer
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                let current_committer_date = revision.committer_date;
                let current_committer_offset = revision.committer_offset;

                let mut valid = false;
                let mut data = env::Revisions::default();
                let mut info = env::CommitAuthor::default();
                if let Some(commit_author) = real_names.get(&current_id){
                    if env::BOTS.contains(&commit_author.author_name.as_str()) || env::BOTS.contains(&commit_author.committer_name.as_str()){
                        amount_bot.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    info = commit_author.clone();
                    
                }
                if let Some(current_date) = current_date {
                    if let Some(current_date_offset) = current_date_offset {
                        data.revisions.push((
                            current_id.clone(),
                            current_date.seconds,
                            current_date_offset,
                            String::from("Author"),
                        ));
                        valid = true;
                    } else {
                        amount_none_offset.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    amount_none_date.fetch_add(1, Ordering::Relaxed);
                }

                if current_author != current_committer {
                    if let Some(current_committer_date) = current_committer_date {
                        if let Some(current_committer_offset) = current_committer_offset {
                            data.revisions.push((
                                current_id.clone(),
                                current_committer_date.seconds,
                                current_committer_offset,
                                String::from("Committer"),
                            ));
                            valid = true;
                        } else {
                            amount_none_committer_offset.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        amount_none_committer_date.fetch_add(1, Ordering::Relaxed);
                    }

                }
                else{
                    if let Some(old_value) = data.revisions.pop(){
                        data.revisions.push((
                            old_value.0,
                            old_value.1,
                            old_value.2,
                            String::from("Both")
                        ));
                    }
                }
                data.revisions.into_iter().for_each(|rev|{
                    let mut metadata = env::Metadata::default();
                    let mut key = String::new();
                    match rev.3.as_str(){
                        "Author" | "Both" =>{
                            metadata.name = info.author_name.clone();
                            metadata.email = info.author_email.clone();
                            key = current_author.clone();
                        }, 
                        "Committer" => {
                            metadata.name = info.committer_name.clone();
                            metadata.email = info.committer_email.clone();
                            key = current_committer.clone();
                        },
                        _ => (),
                    }
                    // todo!("insert only author or committer info not both -> change struct CommitAuthor or Revisions.metadata");
                    all_revisions.upsert(key, 
                        ||{
                            env::Revisions {
                                metadata: metadata.clone(),
                                revisions: vec![rev.clone()],
                            }
                        }, |v|{
                            if v.metadata.name.is_empty(){
                                v.metadata = metadata.clone();
                            }
                            v.revisions.push(rev.clone());
                        });
                });
                if valid {
                    amount_rev_retrieved.fetch_add(1, Ordering::Relaxed);
                }
                amount_rev.fetch_add(1, Ordering::Relaxed);
                bar.set_position(amount_rev.load(Ordering::Relaxed));
            }
        });
    });
    bar.finish_and_clear();
    println!(
        "amount of err date: {}",
        amount_none_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of err timezone: {}",
        amount_none_offset.load(Ordering::Relaxed)
    );
    println!(
        "amount of err committer date: {}",
        amount_none_committer_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of err committer timezone: {}",
        amount_none_committer_offset.load(Ordering::Relaxed)
    );
    println!(
        "amount of err batch: {}",
        amount_overflow_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of bots excluded: {}",
        amount_bot.load(Ordering::Relaxed)
    );
    println!(
        "amount of rev read: {}",
        amount_rev_retrieved.load(Ordering::Relaxed)
    );
    println!("loading time: {:.2?}", start.elapsed());

    // Retrieve both authors
    /* let big_authors = vec![
        String::from("eba34ad04fb96d1226911b5a7bce820126f1cbd9da09a187ccf16bcf6990d893"),
        String::from("42fcc3ed5b24c4780bbcecb719d07dcef72a5881fdb8cdf8ee334b412f107c5b"),
    ];
    for author in big_authors {
        let commits = (*all_revisions.get(&author).unwrap()).clone();
        println!("author: {} | amount of rev: {} | first commit retrieved: {:?} | last commit retrieved: {:?}", author, commits.len(), commits.get(0), commits.last().unwrap());
    } */

    all_revisions
}

/* pub fn load_revisions_from_orc_same_author_committer(opts: &env::Options) -> env::AllRevisions {
    let start = Instant::now();

    let all_revisions = env::AllRevisions::default();

    let files = std::fs::read_dir(&opts.orc_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let amount_rev = AtomicU64::new(0);
    let amount_rev_retrieved = AtomicU64::new(0);
    let amount_none_date = AtomicUsize::new(0);
    let amount_none_offset = AtomicUsize::new(0);
    let amount_none_committer_date = AtomicUsize::new(0);
    let amount_none_committer_offset = AtomicUsize::new(0);
    let amount_overflow_date = AtomicUsize::new(0);
    let bar = ProgressBar::new(opts.amount_revisions as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );

    files.into_par_iter().for_each(|dir_entry| {
        let orc_path = dir_entry.path();
        info!(
            "orc_path : {}",
            orc_path
                .file_name()
                .unwrap()
                .to_os_string()
                .into_string()
                .unwrap()
        );
        debug!("Extension orc OK");
        let file = File::open(dir_entry.path())
            .expect(format!("could not open .orc {:?}", orc_path).as_str());
        let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
        let projection = ProjectionMask::named_roots(
            builder.file_metadata().root_data_type(),
            &[
                "id",
                "message",
                "author",
                "date",
                "date_offset",
                "committer",
                "committer_date",
                "committer_offset",
                "directory",
            ],
        );
        let reader_builder = builder
            .with_projection(projection.clone())
            .with_batch_size(env::ORC_BATCH_SIZE);
        let schema = env::transform_schema(&reader_builder.schema());
        let reader = reader_builder.with_schema(schema).build();
        reader.for_each(|batch| {
            debug!("Entering reader");
            let batch = batch.unwrap();
            for revision in <env::Revision>::from_record_batch(batch).unwrap() {
                let current_author: String = revision
                    .author
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                let current_committer: String = revision
                    .committer
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect();
                if current_author != current_committer {
                    continue;
                }
                let current_id = revision.id;
                let current_date = revision.date;
                let current_date_offset = revision.date_offset;
                let current_committer_date = revision.committer_date;
                let current_committer_offset = revision.committer_offset;

                let mut valid = false;

                if let Some(current_date) = current_date {
                    if let Some(current_date_offset) = current_date_offset {
                        all_revisions.upsert(
                            current_author.clone(),
                            || {
                                vec![(
                                    current_id.clone(),
                                    current_date.seconds,
                                    current_date_offset,
                                )]
                            },
                            |v| {
                                v.push((
                                    current_id.clone(),
                                    current_date.seconds,
                                    current_date_offset,
                                ));
                            },
                        );
                        valid = true;
                    } else {
                        amount_none_offset.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    amount_none_date.fetch_add(1, Ordering::Relaxed);
                }

                if let Some(current_committer_date) = current_committer_date {
                    if current_committer_date != current_date.expect(&format!("couldn't read date when committer date is readable for commit: {current_id}")){
                        if let Some(current_committer_offset) = current_committer_offset {
                            all_revisions.upsert(
                                current_committer,
                                || {
                                    vec![(
                                        current_id.clone(),
                                        current_committer_date.seconds,
                                        current_committer_offset,
                                    )]
                                },
                                |v| {
                                    v.push((
                                        current_id.clone(),
                                        current_committer_date.seconds,
                                        current_committer_offset,
                                    ));
                                },
                            );
                            valid = true;
                        } else {
                            amount_none_committer_offset.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else {
                    amount_none_committer_date.fetch_add(1, Ordering::Relaxed);
                }
                if valid {
                    amount_rev_retrieved.fetch_add(1, Ordering::Relaxed);
                }
                amount_rev.fetch_add(1, Ordering::Relaxed);
                bar.set_position(amount_rev.load(Ordering::Relaxed));
            }
        });
    });
    bar.finish_and_clear();
    println!(
        "amount of err date: {}",
        amount_none_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of err timezone: {}",
        amount_none_offset.load(Ordering::Relaxed)
    );
    println!(
        "amount of err committer date: {}",
        amount_none_committer_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of err committer timezone: {}",
        amount_none_committer_offset.load(Ordering::Relaxed)
    );
    println!(
        "amount of err batch: {}",
        amount_overflow_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of rev read: {}",
        amount_rev_retrieved.load(Ordering::Relaxed)
    );
    println!("loading time: {:.2?}", start.elapsed());

    all_revisions
}
 */

 pub fn load_revisions_from_orc_through_time(opts: &env::Options) -> CHashMap<i32, usize> {
    let start = Instant::now();

    let all_revisions = CHashMap::new();

    let files = std::fs::read_dir(&opts.orc_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let amount_rev = AtomicU64::new(0);
    let amount_rev_retrieved = AtomicU64::new(0);
    let amount_none_committer_date = AtomicUsize::new(0);
    let amount_overflow_date = AtomicUsize::new(0);

    let bar = ProgressBar::new(opts.amount_revisions as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );

    files.into_par_iter().for_each(|dir_entry| {
        let orc_path = dir_entry.path();
        info!(
            "orc_path : {}",
            orc_path
                .file_name()
                .unwrap()
                .to_os_string()
                .into_string()
                .unwrap()
        );
        debug!("Extension orc OK");
        let file = File::open(dir_entry.path())
            .expect(format!("could not open .orc {:?}", orc_path).as_str());
        let builder = ArrowReaderBuilder::try_new(file).expect("could not make builder");
        let projection = ProjectionMask::named_roots(
            builder.file_metadata().root_data_type(),
            &[
                "id",
                "message",
                "author",
                "date",
                "date_offset",
                "committer",
                "committer_date",
                "committer_offset",
                "directory",
            ],
        );
        let reader_builder = builder
            .with_projection(projection.clone())
            .with_batch_size(env::ORC_BATCH_SIZE);
        let schema = env::transform_schema(&reader_builder.schema());
        let reader = reader_builder.with_schema(schema).build();
        reader.for_each(|batch| {
            debug!("Entering reader");
            let batch = batch.unwrap();
            for revision in <env::Revision>::from_record_batch(batch).unwrap() {
                let current_committer_date = revision.committer_date;
                let mut valid = false;

                if let Some(current_committer_date) = current_committer_date {
                    if let Some(date) = DateTime::from_timestamp(current_committer_date.seconds, 0)
                    {
                        let year = date.year();
                        all_revisions.upsert(
                            year,
                            || 1,
                            |cpt| {
                                *cpt += 1;
                            },
                        );
                        valid = true;
                    } else {
                        amount_overflow_date.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    amount_none_committer_date.fetch_add(1, Ordering::Relaxed);
                }

                if valid {
                    amount_rev_retrieved.fetch_add(1, Ordering::Relaxed);
                }
                amount_rev.fetch_add(1, Ordering::Relaxed);
                bar.set_position(amount_rev.load(Ordering::Relaxed));
            }
        });
    });
    bar.finish_and_clear();
    println!(
        "amount of err committer date: {}",
        amount_none_committer_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of overflow date: {}",
        amount_overflow_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of rev read: {}",
        amount_rev_retrieved.load(Ordering::Relaxed)
    );
    println!("loading time: {:.2?}", start.elapsed());

    all_revisions
}

pub fn save_db(all_revisions: env::AllRevisions, opts: &env::Options) {
    let start = Instant::now();
    println!("Saving database");
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    // TODO: do fine tuning on rocksdb options
    let db = rocksdb::DB::open(&options, &opts.output_dir).unwrap();
    let bar = ProgressBar::new(all_revisions.len() as u64);
    bar.set_style(ProgressStyle::with_template("{wide_bar} {eta}").unwrap());
    let mut amount_of_err_serialize = 0;
    let mut amount_of_err_put = 0;
    let mut amount_of_author_put = 0;
    for (author, dates) in all_revisions {
        // let dates = dates.into_inner().unwrap(); // todo if date is a mutex
        match bincode::serialize(&dates) {
            Ok(value) => match db.put(author.clone(), value) {
                Err(err) => {
                    amount_of_err_put += 1;
                    error!("couldn't insert the value for author {author}. Error: {err}");
                }
                Ok(_) => {
                    amount_of_author_put += 1;
                }
            },
            Err(err) => {
                amount_of_err_serialize += 1;
                error!("couldn't serialize the value for author {author}. Error: {err}");
            }
        }
        // Do not deallocate the hashmap and vectors
        std::mem::forget(dates);
        bar.inc(1);
    }
    bar.finish_and_clear();
    println!("DB writing time: {:.2?}", start.elapsed());
    println!("amount_of_author_put = {}", amount_of_author_put);
    println!("amount_of_err_serialize = {}", amount_of_err_serialize);
    println!("amount_of_err_put = {}", amount_of_err_put);
}

pub fn save_revision_year(all_revisions: CHashMap<i32, usize>, opts: &env::Options) {
    let dir_csv = format!("{}/additional", opts.results);
    if let Err(_) = fs::metadata(&dir_csv) {
        fs::create_dir(&dir_csv).expect(&format!("Couldn't create dir: {}", &dir_csv));
    }
    let path_csv = format!("{}/revisions_through_years.csv", dir_csv);
    let mut csv_writer = csv::WriterBuilder::new()
        .flexible(true)
        .from_path(&path_csv)
        .expect(&format!("couldn't create writer at: {}", &path_csv));

    #[derive(Serialize)]
    struct CommitYear {
        year: i32,
        amount_of_commits: usize,
    }
    all_revisions.into_iter().for_each(|rev| {
        csv_writer
            .serialize(CommitYear {
                year: rev.0,
                amount_of_commits: rev.1,
            })
            .expect(&format!("Couldn't write data : {},{}", rev.0, rev.1));
    });
    csv_writer.flush().unwrap();
}
