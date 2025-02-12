use ar_row::Timestamp;
use ar_row_derive::ArRowDeserialize;
use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Schema};
use chashmap::CHashMap;
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Parser)]
pub struct Options {
    /// Directory containing the orc files
    pub orc_dir: String,
    /// Directory containing the database files - no slash at the end
    pub output_dir: String,
    /// path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
    pub results: String,
    /// amount of origins in the graph
    pub amount_revisions: usize,
    /// amount of author
    pub amount_authors: u64,
    pub window_range: u64,
    pub chunk: usize,
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

#[derive(Serialize)]
pub struct Window {
    pub start_index: usize,
    pub start_time: DateTime<Utc>,
    pub end_index: usize,
    pub end_time: DateTime<Utc>,
    pub timezones: Vec<i16>,
}

#[derive(Serialize)]
pub struct Windows {
    pub author: String,
    pub windows: Vec<Window>,
}

// pub struct Date_TZ {
//     pub
// }

pub type AllRevisions = CHashMap<String, Vec<(String, i64, i16)>>;

pub const ONE_DAY: u64 = 24 * 60 * 60;
pub const ONE_WEEK: u64 = 7 * 24 * 60 * 60;
pub const AMOUNT_AUHTOR_2021: u64 = 135_801;
pub const AUTHOR_DB_2024: u64 = 84_117_132;
pub const AUTHOR_DB_2024_EQUALS: u64 = 59_759_065;
pub static ERROR_DATETIME: Mutex<Vec<(String, i64)>> = Mutex::new(Vec::new());

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
