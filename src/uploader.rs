use base64::engine::general_purpose;
use base64::Engine;
use libflate::gzip::Encoder;
use once_cell::sync::Lazy;
use reqwest::{header, Client};
use reth_primitives::BlockNumber;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const REPO: &str = "rkrasiuk/reth-light-sync";

static TOKEN: Lazy<String> =
    Lazy::new(|| std::env::var("GITHUB_TOKEN").expect("failed to read auth token"));
static COMMITER_NAME: Lazy<String> =
    Lazy::new(|| std::env::var("GITHUB_NAME").expect("failed to read github name"));
static COMMITER_EMAIL: Lazy<String> =
    Lazy::new(|| std::env::var("GITHUB_EMAIL").expect("failed to read github email"));

#[derive(Debug)]
pub struct GithubUploader {
    client: Client,
    state_sync_db: PathBuf,
}

impl GithubUploader {
    pub fn new(db_path: &Path) -> Self {
        Self {
            client: Client::default(),
            state_sync_db: db_path.to_owned(),
        }
    }

    pub async fn upload_state(&self, block: BlockNumber) -> eyre::Result<()> {
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&fs::read(&self.state_sync_db.join("mdbx.dat"))?)?;
        let compressed = encoder.finish().into_result()?;

        let message = format!("state snapshot at block #{block}");
        let body = json!({
            "message": message,
            "content": general_purpose::STANDARD.encode(compressed),
            "commiter": {
                "name": &*COMMITER_NAME,
                "email": &*COMMITER_EMAIL,
            }
        });

        let response = self
            .client
            .put(format!(
                "https://api.github.com/repos/{REPO}/contents/snapshots/state-{block}.dat.gz"
            ))
            .bearer_auth(&*TOKEN)
            .header(header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .json(&body)
            .send()
            .await?;

        // TODO: handle
        println!("RESPONSE {:#?}", response.json().await?);

        Ok(())
    }
}
