use reth_db::{
    cursor::DbDupCursorRO,
    mdbx::{Env, WriteMap},
    tables,
    transaction::DbTx,
};
use reth_interfaces::Result;
use reth_primitives::{Account, Address, Bytes, StorageKey, StorageValue, H256, U256};
use reth_provider::{AccountProvider, BlockHashProvider, StateProvider};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct SplitDatabase {
    pub headers_path: PathBuf,
    headers: Arc<Env<WriteMap>>,
    pub bodies_path: PathBuf,
    bodies: Arc<Env<WriteMap>>,
    pub state_path: PathBuf,
    state: Arc<Env<WriteMap>>,
}

impl SplitDatabase {
    pub fn new<H: AsRef<Path>, B: AsRef<Path>, S: AsRef<Path>>(
        headers_path: H,
        headers: Arc<Env<WriteMap>>,
        bodies_path: B,
        bodies: Arc<Env<WriteMap>>,
        state_path: S,
        state: Arc<Env<WriteMap>>,
    ) -> Self {
        let headers_path = headers_path.as_ref().to_owned();
        let bodies_path = bodies_path.as_ref().to_owned();
        let state_path = state_path.as_ref().to_owned();
        Self { headers_path, headers, bodies_path, bodies, state_path, state }
    }

    pub fn headers(&self) -> Arc<Env<WriteMap>> {
        Arc::clone(&self.headers)
    }

    pub fn bodies(&self) -> Arc<Env<WriteMap>> {
        Arc::clone(&self.bodies)
    }

    pub fn state(&self) -> Arc<Env<WriteMap>> {
        Arc::clone(&self.state)
    }
}

/// State provider over latest state that takes tx reference.
pub struct LatestSplitStateProvider<'a, 'b, TX: DbTx<'a>> {
    /// Headers database transaction
    headers_db: &'b TX,
    /// State database transaction
    state_db: &'b TX,
    /// Phantom data over lifetime
    phantom: PhantomData<&'a TX>,
}

impl<'a, 'b, TX: DbTx<'a>> LatestSplitStateProvider<'a, 'b, TX> {
    /// Create new state provider
    pub fn new(headers_db: &'b TX, state_db: &'b TX) -> Self {
        Self { headers_db, state_db, phantom: PhantomData {} }
    }
}

impl<'a, 'b, TX: DbTx<'a>> AccountProvider for LatestSplitStateProvider<'a, 'b, TX> {
    /// Get basic account information.
    fn basic_account(&self, address: Address) -> Result<Option<Account>> {
        self.state_db.get::<tables::PlainAccountState>(address).map_err(Into::into)
    }
}

impl<'a, 'b, TX: DbTx<'a>> BlockHashProvider for LatestSplitStateProvider<'a, 'b, TX> {
    /// Get block hash by number.
    fn block_hash(&self, number: U256) -> Result<Option<H256>> {
        self.headers_db.get::<tables::CanonicalHeaders>(number.to::<u64>()).map_err(Into::into)
    }
}

impl<'a, 'b, TX: DbTx<'a>> StateProvider for LatestSplitStateProvider<'a, 'b, TX> {
    /// Get storage.
    fn storage(&self, account: Address, storage_key: StorageKey) -> Result<Option<StorageValue>> {
        let mut cursor = self.state_db.cursor_dup_read::<tables::PlainStorageState>()?;
        if let Some(entry) = cursor.seek_by_key_subkey(account, storage_key)? {
            if entry.key == storage_key {
                return Ok(Some(entry.value))
            }
        }
        Ok(None)
    }

    /// Get account code by its hash
    fn bytecode_by_hash(&self, code_hash: H256) -> Result<Option<Bytes>> {
        self.state_db
            .get::<tables::Bytecodes>(code_hash)
            .map_err(Into::into)
            .map(|r| r.map(Bytes::from))
    }
}
