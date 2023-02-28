#[derive(Debug)]
pub struct GithubStoreConfig {
    pub owner: String,
    pub repository: String,
    pub token: String,
    pub agent: Option<String>,
    pub name: String,
    pub email: String,
}
