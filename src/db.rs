use reth_db::{
    cursor::DbCursorRO,
    database::Database,
    mdbx::{Env, WriteMap},
    tables,
    transaction::{DbTx, DbTxMut},
    TableType,
};
use reth_db::{mdbx::DatabaseFlags, Error as DatabaseError};
use reth_primitives::{Account, ChainSpec, H256};
use reth_staged_sync::utils::init::InitDatabaseError;
use std::{path::Path, sync::Arc};

pub fn init_db<P: AsRef<Path>>(
    path: P,
    tables: &[(TableType, &str)],
) -> eyre::Result<Arc<Env<WriteMap>>> {
    std::fs::create_dir_all(path.as_ref())?;
    let db = reth_db::mdbx::Env::<reth_db::mdbx::WriteMap>::open(
        path.as_ref(),
        reth_db::mdbx::EnvKind::RW,
    )?;

    let tx = db
        .inner
        .begin_rw_txn()
        .map_err(|e| DatabaseError::InitTransaction(e.into()))?;

    for (table_type, table) in tables {
        let flags = match table_type {
            TableType::Table => DatabaseFlags::default(),
            TableType::DupSort => DatabaseFlags::DUP_SORT,
        };
        tx.create_db(Some(table), flags)
            .map_err(|e| DatabaseError::TableCreation(e.into()))?;
    }
    tx.commit().map_err(|e| DatabaseError::Commit(e.into()))?;

    Ok(Arc::new(db))
}

/// Write the genesis block if it has not already been written
#[allow(clippy::field_reassign_with_default)]
pub fn init_genesis<DB: Database>(
    headers_db: Arc<DB>,
    state_db: Arc<DB>,
    chain: ChainSpec,
) -> Result<H256, InitDatabaseError> {
    let genesis = chain.genesis();

    let header = chain.genesis_header();
    let hash = header.hash_slow();

    let headers_tx = headers_db.tx()?;
    if let Some((_, db_hash)) = headers_tx
        .cursor_read::<tables::CanonicalHeaders>()?
        .first()?
    {
        if db_hash == hash {
            tracing::debug!("Genesis already written, skipping.");
            return Ok(hash);
        }

        return Err(InitDatabaseError::GenesisHashMismatch {
            expected: hash,
            actual: db_hash,
        });
    }

    drop(headers_tx);
    tracing::debug!("Writing genesis block.");
    let headers_tx = headers_db.tx_mut()?;
    headers_tx.put::<tables::CanonicalHeaders>(0, hash)?;
    headers_tx.put::<tables::Headers>(0, header)?;
    headers_tx.commit()?;

    let state_tx = state_db.tx_mut()?;
    // Insert account state
    for (address, account) in &genesis.alloc {
        state_tx.put::<tables::PlainAccountState>(
            *address,
            Account {
                nonce: account.nonce.unwrap_or_default(),
                balance: account.balance,
                bytecode_hash: None,
            },
        )?;
    }
    state_tx.commit()?;

    Ok(hash)
}
