use reth_db::{tables, TableType};

pub const MDBX_DAT: &str = "mdbx.dat";
pub const DAT_GZ_EXT: &str = ".dat.gz";

pub const HEADERS_PREFIX: &str = "headers-";
pub const HEADERS_TABLES: [(TableType, &str); 3] = [
    (TableType::Table, tables::SyncStage::const_name()),
    (TableType::Table, tables::Headers::const_name()),
    (TableType::Table, tables::CanonicalHeaders::const_name()),
];

pub const BODIES_PREFIX: &str = "bodies-";
pub const BODIES_TABLES: [(TableType, &str); 4] = [
    (TableType::Table, tables::BlockBodies::const_name()),
    (TableType::Table, tables::Transactions::const_name()),
    (TableType::Table, tables::BlockOmmers::const_name()),
    (TableType::Table, tables::BlockWithdrawals::const_name()),
];

pub const STATE_PREFIX: &str = "state-snapshots/state-";
pub const STATE_TABLES: [(TableType, &str); 4] = [
    (TableType::Table, tables::SyncStage::const_name()),
    (TableType::Table, tables::PlainAccountState::const_name()),
    (TableType::DupSort, tables::PlainStorageState::const_name()),
    (TableType::Table, tables::Bytecodes::const_name()),
];
