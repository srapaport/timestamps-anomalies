use ar_row::deserialize::ArRowDeserialize;
use indicatif::{ProgressBar, ProgressStyle};
use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;
use rayon::prelude::*;
use std::fs::File;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use timestamps_anomalies::env;

use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::LevelFilter;
use log::{debug, info};

use chrono::{DateTime, Utc};

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ar_row::deserialize::ArRowStruct;
use arrow::array::RecordBatchReader;
use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Schema};
use orc_rust::reader::ChunkReader;

fn main() {
    /* let date = DateTime::from_timestamp(1499772250, 0).unwrap();
    println!("Date: {:?}, time: {:?}", date.date_naive(), date.time()); */
    //test();
    test_orc();
}

fn test_orc() {
    const ORC_BATCH_SIZE: usize = 1024;
    let file = File::open("/poolswh/softwareheritage/graph/2024-12-06/orc/revision/revision-013ebd88-9bea-4e16-839a-47d6deb8b9b2.orc").unwrap();
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
        .with_batch_size(ORC_BATCH_SIZE);
    let schema = transform_schema(&reader_builder.schema());
    let reader = reader_builder.with_schema(schema).build();
    reader.for_each(|batch| {
        let batch = batch.unwrap();
        for _revision in <env::Revision>::from_record_batch(batch).unwrap() {}
    });
}
fn transform_schema(schema: &Schema) -> Arc<Schema> {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .cloned()
            .map(|field| match field.data_type() {
                DataType::Timestamp(_, _) => (*field)
                    .clone()
                    //.with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
                    .with_data_type(DataType::Decimal128(Decimal128Type::MAX_SCALE as _, 9)),
                _ => (*field).clone(),
            })
            .collect::<Vec<_>>(),
    ))
}

fn test() {
    Logger::with(LevelFilter::Info)
        .log_to_file(
            flexi_logger::FileSpec::default()
                // .directory("/infres/ir800/rapaport/results/FULL_new/logs")
                // .basename("FULL-all")
                .directory("./logs")
                .basename("test_orc")
                .suffix("log"),
        )
        .rotate(
            Criterion::Size(10_000_000), // Rotate when the file exceeds 10 MB
            Naming::Numbers,             // Use numbers for rotated files
            Cleanup::KeepLogFiles(5),    // Keep at most 5 log files
        )
        .duplicate_to_stderr(Duplicate::Info) // Duplicate logs to stderr
        .start()
        .unwrap();
    let start = Instant::now();

    let files = std::fs::read_dir("/poolswh/softwareheritage/graph/2024-12-06/orc/revision")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let amount_rev = AtomicU64::new(0);
    let amount_overflow_date = AtomicU64::new(0);
    let amount_none_date = AtomicUsize::new(0);
    let amount_none_committer_date = AtomicUsize::new(0);
    let bar = ProgressBar::new(4_584_279_537);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );

    for dir_entry in files {
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
            match batch {
                Ok(batch) => {
                    for _revision in <env::Revision>::from_record_batch(batch).unwrap() {
                        amount_rev.fetch_add(1, Ordering::Relaxed);
                        bar.set_position(amount_rev.load(Ordering::Relaxed));
                    }
                }
                Err(err) => {
                    debug!("Error batch: {:?}", err.to_string());
                    amount_overflow_date.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    /* files.into_par_iter().for_each(|dir_entry| {

    }); */
    println!(
        "amount of err date: {}",
        amount_none_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of overflow date: {}",
        amount_overflow_date.load(Ordering::Relaxed)
    );
    println!(
        "amount of err committer date: {}",
        amount_none_committer_date.load(Ordering::Relaxed)
    );
    println!("loading time: {:.2?}", start.elapsed());
}
