use timestamps_anomalies::env;

fn main(){
    let opts = env::Options {
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
        chunk: 500_000,
    };
    timestamps_anomalies::commits_year_count(&opts);
}
