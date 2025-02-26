use rocksdb::{Options, DB};
use log::warn;
use indicatif::{ProgressBar, ProgressStyle};
use timestamps_anomalies::env;
fn main(){
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/poolswh/softwareheritage/graph/2024-12-06/orc/revision".to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2024".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        // results: "/home/infres/rapaport/results/timestamps/FULL_2024-12-WEEK".to_string(),
        results: "./results/2024_flight".to_string(),
        amount_revisions: 4_584_279_537,
        amount_authors: env::AUTHOR_DB_2024,
        window_range: env::ONE_DAY,
        chunk: 500_000,
    };
    /* let _jia_and_larhzu = "827ac5b4821491fd3afe0d0e1ddac326253aeb66";
    let jia_alone = "ee1887d2c040f6a6dca18af8479bacf7b922a221";
    let options = Options::default();
    let db = DB::open(&options, "./rocksdb_timezones_2024").unwrap();
    let bar = ProgressBar::new(timestamps_anomalies::env::AUTHOR_DB_2024);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
        )
        .unwrap(),
    );
    for result in db.iterator(rocksdb::IteratorMode::Start){
        match result {
            Ok((key, value)) => {
                let author = String::from_utf8_lossy(&key).to_string();
                let commits: Vec<(String, i64, i16)> =
                    bincode::deserialize(&value[..]).unwrap();
                if commits.into_iter().any(|(commit, _, _)| commit == String::from(jia_alone)){
                    println!("potential Jia Tan: {}", author);
                }
            }
            Err(_) => {
                warn!("data unreadable from rocksdb");
                continue;
            }
        }
        bar.inc(1);
    }
    bar.finish_with_message("done"); */
    let author = String::from("1b786cabdfc334c29e18c4477102640b096c907fae13a43e724396a67f2f753f");
    /* timestamps_anomalies::utils::single_author_window_res(&opts, author).unwrap().into_iter().for_each(|res|{
        println!("Window Something");
    }); */
    timestamps_anomalies::utils::single_author_flight_res(&opts, author).unwrap().into_iter().for_each(|res|{
        println!("Travel Something");
    });
}