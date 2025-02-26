use log::warn;
use rocksdb::{Options, DB};
use indicatif::{ProgressBar, ProgressStyle};
use timestamps_anomalies::env;

fn main(){
    let opts = timestamps_anomalies::env::Options {
        // Directory containing the orc files
        orc_dir: "/poolswh/softwareheritage/graph/2024-12-06/orc/revision".to_string(),
        // Directory containing the database files - no slash at the end
        output_dir: "./rocksdb_timezones_2024".to_string(),
        // path where results are stored - no slash at the end - must exist -> e.g."./results/2023"
        // results: "/home/infres/rapaport/results/timestamps/FULL_2024-12-WEEK".to_string(),
        results: "./results/2024_flight".to_string(),
        amount_revisions: 4_584_279_537,
        amount_authors: env::AUTHOR_DB_2024,
        window_range: env::ONE_DAY,
        chunk: 500_000,
    };
    /* let ts = timestamps_anomalies::load_authors("list_revisions_full.csv");
    let mut found = vec![];
    let bots = vec![
        String::from("dependabot"),
        String::from("renovatebot"),
        String::from("greenkeeper"),
        String::from("snyk-bot"),
        String::from("imgbot"),
        String::from("codeclimate"),
        String::from("codecov-io"),
        String::from("stale-bot"),
        String::from("semantic-release-bot"),
        String::from("github-actions"),
        String::from("gitlab-ci"),
        String::from("mergify"),
        String::from("bors"),
        String::from("allcontributors"),
        String::from("netlify"),
        String::from("heroku"),
        String::from("circleci"),
        String::from("travisci"),
        String::from("jenkins"),
        String::from("appveyor"),
        String::from("azure-pipelines"),
        String::from("restyled-io"),
        String::from("kodiakhq"),
        String::from("depfu"),
        String::from("deepsource-autofix"),
        String::from("pre-commit-ci"),
        String::from("sonarcloud"),
        String::from("lgtm-com"),
        String::from("whitesource"),
        String::from("fossabot"),
        String::from("sourcery-ai"),
        String::from("gitguardian"),
        String::from("dco-bot"),
        String::from("codeowners-bot"),
        String::from("weblate"),
        String::from("zappr"),
        String::from("pyup-bot"),
        String::from("kodiak"),
        String::from("bundler-bot"),
        String::from("codeql-analysis"),
        String::from("eslint-bot"),
        String::from("babelbot"),
        String::from("gh-pages"),
        String::from("versionbot"),
        String::from("stylelint-bot"),
        String::from("probot"),
        String::from("docker-bot"),
        String::from("auto-merge"),
        String::from("github-changelog-generator"),
        String::from("waffle-io"),
        String::from("zenhub"),
        String::from("commitlint"),
        String::from("better-CODE-hub"),
        String::from("bundlewatch"),
        String::from("swagger-api"),
        String::from("release-drafter"),
        String::from("axion-release"),
        String::from("npmpublish"),
        String::from("ghcr-cleanup"),
        String::from("pull-request-size"),
        String::from("reviewdog"),
        String::from("action-slack"),
        String::from("backport-bot"),
        String::from("buildbot"),
        String::from("autoformat"),
        String::from("automerge"),
        String::from("auto-assign"),
        String::from("github-learning-lab"),
        String::from("codeql"),
        String::from("create-pull-request"),
    ];
    ts.into_iter().for_each(|(_, info)|{
        if bots.contains(&info.author_name){
            found.push(info.author_name.clone());
        }
        if bots.contains(&info.committer_name){
            found.push(info.committer_name.clone());
        }
    });
    found.into_iter().for_each(|bot|{
        println!("Found: {}", bot);
    }); */
    let mut maybe_bots = vec![];
    let options = Options::default();
    let db = DB::open(&options, opts.output_dir.as_str()).unwrap();
    let bar = ProgressBar::new(opts.amount_authors);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
        )
        .unwrap(),
    );
    for result in db.iterator(rocksdb::IteratorMode::Start) {
        match result{
            Ok((key, value)) => {
                let author = String::from_utf8_lossy(&key).to_string();
                let commits: Vec<(String, i64, i16)> = bincode::deserialize(&value[..]).unwrap();
                let amount_of_commits = commits.len();
                if amount_of_commits >= env::ALL_LIFE_OF_COMMITS{
                    maybe_bots.push(author);
                }
            },
            Err(e) => warn!("rocksdb err: {}",e),
        }
        bar.inc(1);
    }
    bar.finish_with_message("Done");
    maybe_bots.into_iter().for_each(|bot|{
        println!("Potential bot: {}", bot);
    });
}