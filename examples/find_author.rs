fn main(){
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/poolswh/softwareheritage/graph/2024-12-06/orc/revision".to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2024".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        results: "/home/infres/rapaport/results/timestamps/FULL_2024-12".to_string(),
        amount_revisions: 0,
        amount_authors: 0,
        window_range: 0,
        chunk: 10_000,
    };
    let commits = timestamps_anomalies::utils::single_author_commits(&opts, String::from("a09227853d1dc6b05f5766c8ed052c0a0ef028dcba05339a4151e405affc446b")).unwrap();
    let first_commit = commits.first().unwrap().0.clone();
    let last_commit = commits.last().unwrap().0.clone();
    println!("id: {first_commit}");
    println!("id: {last_commit}");
}