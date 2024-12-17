pub mod env;
pub mod retrieval;
use std::time::Instant;

use log::info;

pub fn retrieve_revision_timezones(opts: &env::Options) {
    let start = Instant::now();
    retrieval::save_db(retrieval::load_revisions_from_orc(&opts.orc_dir), &opts);
    info!("Total retrieval time: {:.2?}", start.elapsed());
}

