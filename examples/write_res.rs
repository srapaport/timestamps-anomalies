use flexi_logger::{Cleanup, Criterion, Logger, Naming, Duplicate};
use log::LevelFilter;
use chrono::{DateTime, Utc};
use rand::Rng;
use csv::{ReaderBuilder, StringRecord};
use timestamps_anomalies::env;

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

    // let mut res = generate_test_sample(8);

    let mut rdr = ReaderBuilder::new().has_headers(false).from_path("teaser_ds.csv").unwrap();

    let records = rdr
        .records()
        .collect::<Result<Vec<StringRecord>, csv::Error>>().unwrap();
    
    // struct author name, author email, committer name, committer email
    // hashmap <commit id, struct commit>
    // allow to find quickly one commit info with the id among 2M commit
    records.into_iter().for_each(|record| {
        let mut commit_author = env::CommitAuthor::default();
        let mut commit_id = String::new();
        let mut field_id = 0;
        record.iter().for_each(|field| {
            match field_id {
                0 => {
                    commit_id = String::from(field);
                    field_id += 1;
                }
                1 | 2 => {
                    let cleaned = field.strip_prefix("b").unwrap_or(field).trim_matches('\'');
                    if let Some(email_start) = cleaned.rfind('<') {
                        let name = cleaned[..email_start].trim();
                        let email_end;
                        match cleaned[email_start..].find('>') {
                            Some(index) => email_end = email_start + index,
                            None => {
                                println!("{cleaned} | len: {}", cleaned.len());
                                println!("last char: {}", cleaned.chars().nth(cleaned.len() - 1).unwrap_or_default());
                                println!("beginning email: {}", cleaned[email_start + 1..email_start + 2].trim());
                                
                                email_end = cleaned.len() - 1
                            },
                        }
                        // let email_end = email_start + email_end;
                        let email = cleaned[email_start + 1..email_end].trim();
                        if field_id == 1 {
                            commit_author.author_name = String::from(name);
                            commit_author.author_email = String::from(email);
                        } else {
                            commit_author.committer_name = String::from(name);
                            commit_author.committer_email = String::from(email);
                        }
                    } else {
                        // No difference name / email
                        if field_id == 1 {
                            commit_author.author_name = String::from(cleaned);
                            commit_author.author_email = String::from(cleaned);
                        } else {
                            commit_author.committer_name = String::from(cleaned);
                            commit_author.committer_email = String::from(cleaned);
                        }
                    }
                    field_id += 1;
                }
                _ => (),
            }
        });
        println!("{},{},{},{},{}", commit_id, commit_author.author_name, commit_author.author_email, commit_author.committer_name, commit_author.committer_email);
    });
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