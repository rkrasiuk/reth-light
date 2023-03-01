use reth_db::mdbx::{Env, WriteMap};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct SplitDatabase {
    pub headers_path: PathBuf,
    pub state_path: PathBuf,
    headers: Arc<Env<WriteMap>>,
    state: Arc<Env<WriteMap>>,
}

impl SplitDatabase {
    pub fn new<H: AsRef<Path>, S: AsRef<Path>>(
        headers_path: H,
        headers: Arc<Env<WriteMap>>,
        state_path: S,
        state: Arc<Env<WriteMap>>,
    ) -> Self {
        let headers_path = headers_path.as_ref().to_owned();
        let state_path = state_path.as_ref().to_owned();
        Self { headers_path, headers, state_path, state }
    }

    pub fn headers(&self) -> Arc<Env<WriteMap>> {
        Arc::clone(&self.headers)
    }

    pub fn state(&self) -> Arc<Env<WriteMap>> {
        Arc::clone(&self.state)
    }
}
