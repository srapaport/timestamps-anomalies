use clap::Parser;
use chashmap::CHashMap;
use std::collections::HashSet;

#[derive(Parser)]
pub struct Options {
    /// Directory containing the orc files
    pub orc_dir: String,
    /// Directory containing the database files - no slash at the end
    pub output_dir: String,
    /// path of the compressed graph -> e.g. "./datasets/2021-03-23-popular-3k-python-graph/graph"
    pub graph: String,
    /// path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
    pub results: String,
    /// amount of origins in the graph
    pub expected_origins: usize,
    /// amount of origin with an altered history per result file
    pub chunk: usize,
    // Wether we include removed branch or not,
    pub removed_branch: bool,
}

pub type AllRevisions = CHashMap<String, HashSet<(String, i64, i16)>>;