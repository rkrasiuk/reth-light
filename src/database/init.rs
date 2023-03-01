use super::MDBX_DAT;
use reth_db::{
    mdbx::{DatabaseFlags, Env, EnvKind, WriteMap},
    Error as DatabaseError, TableType,
};
use std::{fs, path::Path, sync::Arc};

pub fn restore_database<P: AsRef<Path>>(
    path: P,
    contents: &[u8],
) -> eyre::Result<Arc<Env<WriteMap>>> {
    fs::write(path.as_ref().join(MDBX_DAT), contents)?;
    let db = Env::<WriteMap>::open(path.as_ref(), reth_db::mdbx::EnvKind::RW)?;
    return Ok(Arc::new(db))
}

pub fn init_database<P: AsRef<Path>>(
    path: P,
    tables: &[(TableType, &str)],
) -> eyre::Result<Arc<Env<WriteMap>>> {
    std::fs::create_dir_all(path.as_ref())?;

    let db = Env::<WriteMap>::open(path.as_ref(), EnvKind::RW)?;

    let tx = db.inner.begin_rw_txn().map_err(|e| DatabaseError::InitTransaction(e.into()))?;
    for (table_type, table) in tables {
        let flags = match table_type {
            TableType::Table => DatabaseFlags::default(),
            TableType::DupSort => DatabaseFlags::DUP_SORT,
        };
        tx.create_db(Some(table), flags).map_err(|e| DatabaseError::TableCreation(e.into()))?;
    }
    tx.commit().map_err(|e| DatabaseError::Commit(e.into()))?;

    Ok(Arc::new(db))
}
