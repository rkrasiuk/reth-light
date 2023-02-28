use crate::remote::{
    config::GithubStoreConfig,
    models::{Committer, ContentInfo, ContentRequest},
};
use base64::{engine::general_purpose, Engine};
use libflate::gzip::Encoder;
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, StatusCode, Url,
};
use std::io::Write;

#[derive(Debug)]
pub struct GithubRemoteStore {
    client: Client,
    base_url: Url,
    committer: Committer,
}

impl GithubRemoteStore {
    // API base url
    const REPOS_API_URL: &str = "https://api.github.com/repos";

    // Header entries
    const API_VERSION_HEADER: &str = "X-GitHub-Api-Version";
    const API_VERSION: &str = "2022-11-28";
    const ACCEPT_APPLICATION_CONTENT: &str = "application/vnd.github+json";

    pub fn new(config: GithubStoreConfig) -> eyre::Result<Self> {
        let GithubStoreConfig { agent, email, name, owner, repository, token } = config;

        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static(Self::ACCEPT_APPLICATION_CONTENT));
        headers.insert(Self::API_VERSION_HEADER, HeaderValue::from_static(Self::API_VERSION));

        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", token))?;
        bearer.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, bearer);

        let agent = agent.as_ref().unwrap_or(&owner);

        let url = Self::REPOS_API_URL;
        let base_url = format!("{url}/{owner}/{repository}/");

        Ok(Self {
            client: Client::builder().user_agent(agent).default_headers(headers).build()?,
            base_url: Url::parse(&base_url)?,
            committer: Committer { name, email },
        })
    }

    pub async fn list(&self, path: &str) -> eyre::Result<Vec<ContentInfo>> {
        let url = self.base_url.join("contents/")?.join(path)?;
        tracing::trace!(target: "remote",  %url, "Listing entries");
        let response = self.client.get(url.clone()).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            Ok(Vec::default())
        } else {
            Ok(response.json().await?)
        }
    }

    pub async fn retrieve(&self, path: &str) -> eyre::Result<Option<String>> {
        let url = self.base_url.join("contents/")?.join(path)?;
        tracing::trace!(target: "remote", %url, "Retrieving file");
        let response = self.client.get(url.clone()).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            let ContentInfo { content, .. } = response.json().await?;
            let decoded = content
                .lines()
                .map(|line| Ok(String::from_utf8(general_purpose::STANDARD.decode(line)?)?))
                .collect::<eyre::Result<Vec<_>>>()?;
            Ok(Some(decoded.join("")))
        }
    }

    pub async fn save(&self, path: &str, content: Vec<u8>, message: &str) -> eyre::Result<()> {
        tracing::trace!(target: "remote", path, "Compressing file");
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&content)?;
        let compressed = encoder.finish().into_result()?;

        tracing::trace!(target: "remote", path, "Encoding file");
        let content = general_purpose::STANDARD.encode(compressed);

        let body = ContentRequest {
            message: message.to_owned(),
            content: Some(content),
            committer: self.committer.clone(),
        };

        let url = self.base_url.join("contents/")?.join(path)?;
        tracing::trace!(target: "remote", %url, "Uploading file");
        let response = self.client.put(url.clone()).json(&body).send().await?;

        // TODO: handle response
        if !response.status().is_success() {
            tracing::info!(target: "remote", url = %url, "Saved file");
            Ok(())
        } else {
            let response = response.text().await?;
            tracing::error!(target: "remote", %url, response, "Failed to save file");
            eyre::bail!("failed to save")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_remote_store() -> GithubRemoteStore {
        dotenv::dotenv().ok();
        reth_tracing::init_test_tracing();
        GithubRemoteStore::new(GithubStoreConfig {
            email: "rokrassyuk@gmail.com".to_owned(),
            name: "Roman Krasiuk".to_owned(),
            owner: "rkrasiuk".to_owned(),
            repository: "reth-light-sync".to_owned(),
            token: std::env::var("GITHUB_TOKEN").expect("failed to read auth token"),
            agent: None,
        })
        .expect("failed to create client")
    }

    #[tokio::test]
    async fn retrieve_non_existent() {
        let remote = create_remote_store();
        assert_eq!(remote.retrieve("non-existent").await.unwrap(), None);
    }

    #[tokio::test]
    async fn retrieve_readme() {
        let remote = create_remote_store();
        let readme = remote.retrieve("README.md").await.unwrap();
        assert!(readme.is_some());
        assert!(readme.unwrap().starts_with("# reth-light-sync"));
    }
}
