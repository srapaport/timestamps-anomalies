use chrono::DateTime;
fn main(){
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/poolswh/softwareheritage/graph/2024-12-06/orc/revision".to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2024".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        results: "./results/2024".to_string(),
        amount_revisions: 0,
        amount_authors: 0,
        window_range: 0,
        chunk: 10_000,
    };
    let _opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/home/infres/rapaport/datasets/2021-03-23-popular-3k-python-orc/revision"
            .to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2021".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        results: "./test".to_string(),
        amount_revisions: 5_973_059,
        amount_authors: timestamps_anomalies::env::AMOUNT_AUHTOR_2021,
        window_range: timestamps_anomalies::env::ONE_DAY,
        chunk: 500,
    };
    /* let commits = timestamps_anomalies::utils::single_author_commits(&opts, String::from("cb95ced0e6d2f343e5d7b6f267c3c404ac4e04250ab01bfbd484b87433ad1db4")).unwrap();
    let first_commit = commits.first().unwrap().0.clone();
    let last_commit = commits.last().unwrap().0.clone();
    println!("id: {first_commit}");
    println!("id: {last_commit}"); */
    let author = String::from("cb95ced0e6d2f343e5d7b6f267c3c404ac4e04250ab01bfbd484b87433ad1db4");
    let start_index = 2163;
    let end_index = 2168;
    let mut cpt = start_index;
    timestamps_anomalies::utils::get_author_commits_range(&opts, author, start_index, end_index).unwrap()
        .into_iter()
        .for_each(|commit|{
            let date = DateTime::from_timestamp(commit.1, 0).unwrap();
            println!("index: {} | commit id : {} | time: {:?} {:?} | timezone: {}", cpt, commit.0, date.date_naive(), date.time(), commit.2);
            cpt += 1;
        });

}