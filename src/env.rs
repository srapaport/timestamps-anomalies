use ar_row::Timestamp;
use ar_row_derive::ArRowDeserialize;
use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Schema};
use chashmap::CHashMap;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    pub amount_revisions: usize,
    /// amount of origin with an altered history per result file
    pub chunk: usize,
    // Wether we include removed branch or not,
    pub removed_branch: bool,
}

#[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct Revision {
    pub id: String,
    pub message: Option<Box<[u8]>>,
    pub author: Box<[u8]>,
    pub date: Option<Timestamp>,
    pub date_offset: Option<i16>,
    pub committer: Box<[u8]>,
    pub committer_date: Option<Timestamp>,
    pub committer_offset: Option<i16>,
    pub directory: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorCommit {
    pub author: String,
    pub commit_id: String,
    pub timestamp_utc0: i64,
    pub timezone: i16,
}

// pub struct Date_TZ {
//     pub
// }

pub type AllRevisions = CHashMap<String, Vec<(String, i64, i16)>>;

pub const ONE_DAY: u64 = 24 * 60 * 60;
pub const ONE_WEEK: u64 = 7 * 24 * 60 * 60;
pub const AMOUNT_AUHTOR_2021: u64 = 135_801;
pub const AUTHOR_DB_2024: u64 = 84_117_132;

pub const ORC_BATCH_SIZE: usize = 1024;
pub fn transform_schema(schema: &Schema) -> Arc<Schema> {
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
