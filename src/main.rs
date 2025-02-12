use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::LevelFilter;
use std::time::Instant;
use timestamps_anomalies::env;

fn main() {
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/poolswh/softwareheritage/graph/2024-12-06/orc/revision".to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2024".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        // results: "/home/infres/rapaport/results/timestamps/FULL_2024-12-WEEK".to_string(),
        results: "./results/2024".to_string(),
        amount_revisions: 4_584_279_537,
        amount_authors: env::AUTHOR_DB_2024,
        window_range: env::ONE_DAY,
        chunk: 100_000,
    };
    let _opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/home/infres/rapaport/datasets/2021-03-23-popular-3k-python-orc/revision"
            .to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2021".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        results: "./results/2021".to_string(),
        amount_revisions: 5_973_059,
        amount_authors: env::AMOUNT_AUHTOR_2021,
        window_range: env::ONE_DAY,
        chunk: 500,
    };
    Logger::with(LevelFilter::Info)
        .log_to_file(
            flexi_logger::FileSpec::default()
                // .directory("/infres/ir800/rapaport/results/FULL_new/logs")
                // .basename("FULL-all")
                .directory(format!("{}/logs", opts.results))
                .basename("window")
                .suffix("log"),
        )
        .rotate(
            Criterion::Size(10_000_000), // Rotate when the file exceeds 10 MB
            Naming::Numbers,             // Use numbers for rotated files
            Cleanup::KeepLogFiles(5),    // Keep at most 5 log files
        )
        .duplicate_to_stderr(Duplicate::Error) // Duplicate logs to stderr
        .start()
        .unwrap();
    let start = Instant::now();

    timestamps_anomalies::window_tz_inconsistency(&opts);

    println!("time elapsed: {:.2?}", start.elapsed());
}
