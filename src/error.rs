use thiserror::Error;

#[derive(Debug, Error)]
pub enum HakoError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("lock poisoned: {0}")]  // <--- ADD THIS
    LockPoisoned(String),
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("query error: {0}")]
    QueryError(String),
    #[error("index error: {0}")]
    IndexError(String),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, HakoError>;
