use crate::env;
use ar_row::deserialize::ArRowDeserialize;
use indicatif::{ProgressBar, ProgressStyle};
use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;
use rayon::prelude::*;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use log::{debug, info};

pub fn load_revisions_from_orc(opts: &env::Options) -> env::AllRevisions {
    let start = Instant::now();

    let all_revisions = env::AllRevisions::default();

    let files = std::fs::read_dir(&opts.orc_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let amount_rev = AtomicU64::new(0);
    // let bar = ProgressBar::new(3_500_228_278);
    let bar = ProgressBar::new(5_973_059);
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
        let reader = builder.with_projection(projection).build();
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
                /////////////////// TEST
                /* if current_id == "fe82c46327effc124ff166e1fa1e611579e1176b".to_string(){
                    println!("{}",current_author);
                    println!("{:?}",DateTime::from_timestamp(current_date.seconds, current_date.nanoseconds.try_into().unwrap()));
                    println!("seconds: {}, nanoseconds: {}", current_date.seconds, current_date.nanoseconds);
                    println!("{}",current_date_offset);
                    println!("{}",current_committer);
                    println!("{:?}",DateTime::from_timestamp(current_committer_date.seconds, current_committer_date.nanoseconds.try_into().unwrap()));
                    println!("seconds: {}, nanoseconds: {}", current_committer_date.seconds, current_committer_date.nanoseconds);
                    println!("{}",current_committer_offset);
                } */
                /////////////////// TEST
                // let current_date_offset_minutes: i64 = (current_date_offset * 60).into();
                //let current_date_utc0: DateTime<Utc> = DateTime::from_timestamp(current_date.seconds - current_date_offset_minutes, 0).unwrap();
                //let current_date_utc0 = current_date.seconds - current_date_offset_minutes;
                all_revisions.upsert(
                    current_author.clone(),
                    || {
                        vec![(current_id.clone(), current_date.seconds, current_date_offset)]
                    },
                    |v| {
                        v.push((current_id.clone(), current_date.seconds, current_date_offset));
                    },
                );
                if current_author != current_committer {
                    // let current_committer_offset_minutes: i64 = (current_committer_offset * 60).into();
                    /* let current_committer_date_utc0: DateTime<Utc> =
                        DateTime::from_timestamp(current_committer_date.seconds - current_committer_offset_minutes, 0); */
                    // let current_committer_date_utc0= current_committer_date.seconds - current_committer_offset_minutes;
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
                }
                amount_rev.fetch_add(1, Ordering::Relaxed);
                bar.set_position(amount_rev.load(Ordering::Relaxed));
            }
        });
    });

    // all_revisions.into_iter().for_each(|(k,v)|{
    //     println!("Author: {:?}", k);
    //     for elt in v{
    //         println!("  rev id: {:?}", elt.0);
    //         println!("  date: {:?}", elt.1);
    //         println!("  offset: {:?}", elt.2);
    //     }
    // });
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
    for (author, dates) in all_revisions {
        // let dates = dates.into_inner().unwrap(); // todo if date is a mutex
        db.put(author, bincode::serialize(&dates).unwrap()).unwrap();
        // Do not deallocate the hashmap and vectors
        std::mem::forget(dates);
        bar.inc(1);
    }
    bar.finish_and_clear();
    println!("DB writing time: {:.2?}", start.elapsed());
}
