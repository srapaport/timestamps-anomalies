use flexi_logger::{Cleanup, Criterion, Logger, Naming};
use log::LevelFilter;
use std::time::Instant;

fn main() {
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        // orc_dir: "/home/infres/rapaport/datasets/2021-03-23-popular-3k-python-orc/revision"
        //     .to_string(),
        orc_dir: "/poolswh/softwareheritage/graph/2024-12-06/orc/revision".to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2024".to_string(),
        // path of the compressed graph -> e.g. "./datasets/2021-03-23-popular-3k-python-graph/graph"
        graph: String::new(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        results: String::new(),
        // amount of origins in the graph
        //amount_revisions: 5_973_059,
        amount_revisions: 4_584_279_537,
        // amount of origin with an altered history per result file
        chunk: 10_000,
        // Wether we include removed branch or not,
        removed_branch: false,
    };
    Logger::with(LevelFilter::Info)
        .log_to_file(
            flexi_logger::FileSpec::default()
                // .directory("/infres/ir800/rapaport/results/FULL_new/logs")
                // .basename("FULL-all")
                .directory("./logs")
                .basename("test_orc_bis")
                .suffix("log"),
        )
        .rotate(
            Criterion::Size(10_000_000), // Rotate when the file exceeds 10 MB
            Naming::Numbers,             // Use numbers for rotated files
            Cleanup::KeepLogFiles(5),    // Keep at most 5 log files
        )
        //.duplicate_to_stderr(Duplicate::Info) // Duplicate logs to stderr
        .start()
        .unwrap();
    let start = Instant::now();
    // timestamps_anomalies::retrieve_revision_timezones(&opts);
    // timestamps_anomalies::retrieval::load_revisions_from_orc(&opts);
    // timestamps_anomalies::window_tz_inconsistency(&opts);

    /* let author = String::from("08bc61697d00da1f6582634608026cd4e0d372486331189fbb9c8d33ab2486f4");
    println!("windows for author {}", author);
    match timestamps_anomalies::utils::single_author_window_res(&opts, author) {
        Some(windows) => {
            let mut amount_of_window = 0;
            for window in windows {
                amount_of_window += 1;
                println!("window: {}", amount_of_window);
                window.into_iter().for_each(|date|{
                    println!("{}, {:?}, {:?}, {}", date.0, date.1.date_naive(), date.1.time(), date.2);
                });
            }
        }
        None => println!("no window"),
    } */
    println!("time elapsed: {:.2?}", start.elapsed());
}
