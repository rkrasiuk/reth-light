use crate::{database::init::restore_database, remote::digitalocean::store::DigitalOceanStore};
use itertools::Itertools;
use reth_db::{
    cursor::DbCursorRO,
    database::Database,
    mdbx::{Env, WriteMap},
    tables,
    transaction::{DbTx, DbTxMut},
    TableType,
};
use reth_primitives::{Account, ChainSpec};
use reth_staged_sync::utils::init::InitDatabaseError;
use reth_stages::stages::{EXECUTION, HEADERS};
use std::{path::Path, sync::Arc};

mod init;
use init::init_database;

pub mod provider;

pub const MDBX_DAT: &str = "mdbx.dat";

pub const HEADERS_TABLES: [(TableType, &str); 3] = [
    (TableType::Table, tables::SyncStage::const_name()),
    (TableType::Table, tables::Headers::const_name()),
    (TableType::Table, tables::CanonicalHeaders::const_name()),
];

pub const STATE_TABLES: [(TableType, &str); 4] = [
    (TableType::Table, tables::SyncStage::const_name()),
    (TableType::Table, tables::PlainAccountState::const_name()),
    (TableType::DupSort, tables::PlainStorageState::const_name()),
    (TableType::Table, tables::Bytecodes::const_name()),
];

fn header_snaphot_progress(name: &str) -> u64 {
    let block_str = name.strip_prefix("headers-").unwrap().strip_suffix(".dat.gz").unwrap();
    let block: u64 = block_str.parse().unwrap();
    block
}

pub async fn init_headers_db<P: AsRef<Path>>(
    path: P,
    remote: &DigitalOceanStore,
    chain_spec: ChainSpec,
) -> eyre::Result<Arc<Env<WriteMap>>> {
    let local = init_database(&path, &HEADERS_TABLES)?;
    let progress = HEADERS.get_progress(&local.tx()?)?.unwrap_or_default();
    let snapshots = remote.list(Some("headers-")).await?;
    let db = {
        let best_snapshot = snapshots
            .into_iter()
            .map(|s| (header_snaphot_progress(s.key().unwrap()), s))
            .sorted_by_key(|s| s.0)
            .rev()
            .next()
            .filter(|s| s.0 > progress);
        if let Some((_, snapshot)) = best_snapshot {
            drop(local);
            let raw = remote.retrieve(snapshot.key().unwrap()).await?.unwrap();
            restore_database(path, &raw)?
        } else {
            local
        }
    };

    let header = chain_spec.genesis_header();
    let hash = header.hash_slow();

    if let Some((_, db_hash)) =
        db.view(|tx| tx.cursor_read::<tables::CanonicalHeaders>()?.first())??
    {
        if db_hash == hash {
            tracing::debug!("Genesis already written, skipping.");
            return Ok(db)
        }

        return Err(InitDatabaseError::GenesisHashMismatch { expected: hash, actual: db_hash }.into())
    }

    tracing::debug!("Writing genesis block.");
    db.update(|tx| {
        tx.put::<tables::CanonicalHeaders>(0, hash)?;
        tx.put::<tables::Headers>(0, header.clone())
    })??;

    Ok(db)
}

pub async fn init_state_db<P: AsRef<Path>>(
    path: P,
    _remote: &DigitalOceanStore,
    chain_spec: ChainSpec,
) -> eyre::Result<Arc<Env<WriteMap>>> {
    // TODO: check remote

    let db = init_database(path, &STATE_TABLES)?;

    let progress = EXECUTION.get_progress(&db.tx()?)?;
    if progress.is_none() {
        db.update(|tx| {
            // Insert account state
            chain_spec.genesis().alloc.iter().try_for_each(|(address, account)| {
                tx.put::<tables::PlainAccountState>(
                    *address,
                    Account {
                        nonce: account.nonce.unwrap_or_default(),
                        balance: account.balance,
                        bytecode_hash: None,
                    },
                )
            })
        })??;
    }

    Ok(db)
}
