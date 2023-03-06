use super::{BODIES_TABLES, HEADERS_TABLES, STATE_TABLES};
use reth_db::{
    cursor::DbCursorRO,
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
    TableType,
};
use reth_primitives::{Account, BlockNumber, ChainSpec};
use reth_staged_sync::utils::init::InitDatabaseError;
use reth_stages::stages::{BODIES, EXECUTION, HEADERS};

pub trait DatabaseDescriptor<DB: Database> {
    fn default_tables(&self) -> &[(TableType, &str)];

    fn progress(&self, db: DB) -> eyre::Result<Option<BlockNumber>>;

    fn ensure_genesis(&self, db: DB, chain_spec: ChainSpec) -> eyre::Result<()>;
}

pub struct HeadersDescriptor;
impl<DB: Database> DatabaseDescriptor<DB> for HeadersDescriptor {
    fn default_tables(&self) -> &[(TableType, &str)] {
        &HEADERS_TABLES
    }

    fn progress(&self, db: DB) -> eyre::Result<Option<BlockNumber>> {
        let tx = db.tx()?;
        Ok(HEADERS.get_progress(&tx)?)
    }

    fn ensure_genesis(&self, db: DB, chain_spec: ChainSpec) -> eyre::Result<()> {
        let header = chain_spec.genesis_header();
        let hash = header.hash_slow();

        let inserted_hash =
            db.view(|tx| tx.cursor_read::<tables::CanonicalHeaders>()?.first())??;
        if let Some((_, db_hash)) = inserted_hash {
            if db_hash == hash {
                tracing::debug!("Genesis already written, skipping.");
                return Ok(())
            }
            return Err(
                InitDatabaseError::GenesisHashMismatch { expected: hash, actual: db_hash }.into()
            )
        }

        tracing::debug!("Writing genesis block.");
        db.update(|tx| {
            tx.put::<tables::CanonicalHeaders>(0, hash)?;
            tx.put::<tables::Headers>(0, header.clone())
        })??;
        Ok(())
    }
}

pub struct BodiesDescriptor;
impl<DB: Database> DatabaseDescriptor<DB> for BodiesDescriptor {
    fn default_tables(&self) -> &[(TableType, &str)] {
        &BODIES_TABLES
    }

    fn progress(&self, db: DB) -> eyre::Result<Option<BlockNumber>> {
        let tx = db.tx()?;
        Ok(BODIES.get_progress(&tx)?)
    }

    fn ensure_genesis(&self, db: DB, _chain_spec: ChainSpec) -> eyre::Result<()> {
        let progress =
            db.view(|tx| tx.get::<tables::SyncStage>(BODIES.0.as_bytes().to_vec()))??;
        if progress.is_none() {
            db.update(|tx| tx.put::<tables::BlockBodies>(0, Default::default()))??;
        }
        Ok(())
    }
}

pub struct StateDescriptor;
impl<DB: Database> DatabaseDescriptor<DB> for StateDescriptor {
    fn default_tables(&self) -> &[(TableType, &str)] {
        &STATE_TABLES
    }

    fn progress(&self, db: DB) -> eyre::Result<Option<BlockNumber>> {
        let tx = db.tx()?;
        Ok(EXECUTION.get_progress(&tx)?)
    }

    fn ensure_genesis(&self, db: DB, chain_spec: ChainSpec) -> eyre::Result<()> {
        let progress =
            db.view(|tx| tx.get::<tables::SyncStage>(EXECUTION.0.as_bytes().to_vec()))??;
        if progress.is_none() {
            db.update(|tx| {
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
        Ok(())
    }
}
