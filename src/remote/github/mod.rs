pub mod config;
pub mod models;
pub mod store;

pub async fn list_headers_snapshots(
    remote: &store::GithubRemoteStore,
) -> eyre::Result<Vec<models::ContentInfo>> {
    Ok(remote
        .list("")
        .await?
        .into_iter()
        .filter(|e| e.path.starts_with("headers-") && e.path.ends_with(".dat.gz"))
        .collect())
}
