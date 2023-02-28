use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ContentRequest {
    pub message: String,
    pub committer: Committer,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Committer {
    pub name: String,
    pub email: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContentInfo {
    pub download_url: String,
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub content: String,
}
