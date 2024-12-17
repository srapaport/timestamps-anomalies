fn main() {
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/home/infres/rapaport/datasets/2021-03-23-popular-3k-python-orc/revision"
            .to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2021".to_string(),
        // path of the compressed graph -> e.g. "./datasets/2021-03-23-popular-3k-python-graph/graph"
        graph: String::new(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        results: String::new(),
        // amount of origins in the graph
        expected_origins: 0,
        // amount of origin with an altered history per result file
        chunk: 0,
        // Wether we include removed branch or not,
        removed_branch: false,
    };
    timestamps_anomalies::retrieve_revision_timezones(&opts);
}
