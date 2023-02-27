use reth::dirs::{data_dir, XdgPath};
use std::path::PathBuf;

#[derive(Default, Debug, Clone)]
pub struct HeadersDbPath;

impl XdgPath for HeadersDbPath {
    fn resolve() -> Option<PathBuf> {
        data_dir().map(|root| root.join("headers-db"))
    }
}

#[derive(Default, Debug, Clone)]
pub struct StateDbPath;

impl XdgPath for StateDbPath {
    fn resolve() -> Option<PathBuf> {
        data_dir().map(|root| root.join("state-db"))
    }
}
