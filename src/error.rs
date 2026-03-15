use thiserror::Error;

#[derive(Debug, Error)]
pub enum FireLiteError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("not found")]
    NotFound,
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, FireLiteError>;
