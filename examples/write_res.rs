use flexi_logger::{Cleanup, Criterion, Logger, Naming, Duplicate};
use log::LevelFilter;
use chrono::{DateTime, Utc};
use rand::Rng;

fn main(){
    Logger::with(LevelFilter::Debug)
        .log_to_file(
            flexi_logger::FileSpec::default()
                // .directory("/infres/ir800/rapaport/results/FULL_new/logs")
                // .basename("FULL-all")
                .directory("./test/logs")
                .basename("test")
                .suffix("log"),
        )
        .rotate(
            Criterion::Size(10_000_000), // Rotate when the file exceeds 10 MB
            Naming::Numbers,             // Use numbers for rotated files
            Cleanup::KeepLogFiles(5),    // Keep at most 5 log files
        )
        .duplicate_to_stderr(Duplicate::Debug) // Duplicate logs to stderr
        .start()
        .unwrap();

    let mut res = generate_test_sample(8);
    // crate::window::flush_res("./test", &mut res);
}

fn generate_commits(amount_of_window: usize)-> Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>{
    let mut commits = Vec::new();
    for _ in 0..amount_of_window{
        let size = rand::random_range(3..=10);
        let mut random_vec: Vec<i16> = vec![];
        for _ in 0..size {
            random_vec.push(rand::random_range(-1000..=1000));
        }
        let i: usize = rand::random_range(0..100);
        let j: usize = rand::random_range(i+1..i+100);
        let i_date = rand::random_range(946684800..Utc::now().timestamp());
        let j_date = rand::random_range(i_date..i_date+100_000);
        commits.push(((i, DateTime::from_timestamp(i_date,0).unwrap()),(j, DateTime::from_timestamp(j_date,0).unwrap()),random_vec));
    }
    commits
}

fn generate_author()->String{
    const HEX_CHARS: &[u8] = b"0123456789abcdef";
    let mut rng = rand::rng();
    (0..64)
        .map(|_| {
            let idx = rng.random_range(0..HEX_CHARS.len());
            HEX_CHARS[idx] as char
        })
        .collect()
}

fn generate_test_sample(amount_of_author: usize) -> Vec<(String, Vec<((usize, DateTime<Utc>), (usize, DateTime<Utc>), Vec<i16>)>)>{
    let mut test = Vec::new();
    for _ in 0..amount_of_author{
        let amount_of_window = rand::random_range(1..=100);
        test.push((generate_author(), generate_commits(amount_of_window)));
    }
    test
}