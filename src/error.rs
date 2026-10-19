use thiserror::Error;

/// Error classification for FFI consumers (nothing is exposed on the wire
/// except this code — SDK retry loops must not string-match, issue #71/F5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorKind {
    /// No error recorded on this thread.
    None = 0,
    /// Normal failure — do not retry as-is.
    Generic = 1,
    /// Retryable without repair: serializable-transaction read-set conflict
    /// or an index still being backfilled. Retry with the SAME input after
    /// a backoff; the engine state may legitimately have advanced.
    RetryableConflict = 2,
    /// Engine-enforced constraint violation — retrying unchanged input
    /// deterministically fails again; the WRITE is the fix.
    UniqueViolation = 3,
    /// Constraint target is not comparable against its declared constraint
    /// (schema drift) — repair by aligning the write.
    ConstraintMismatch = 4,
}

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
    /// Serializable-transaction read-set validation failed. A NORMAL
    /// concurrency outcome (two writers raced on a read-modify-write),
    /// never corruption (issue #71/F5). Retry the whole transaction.
    #[error("transaction conflict on `{key}`")]
    Conflict {
        key: String,
    },
    /// A constrained (unique) index rejected the write (issue #71/F1).
    /// `key` is the offending tuple, repr'd for diagnostics only — do NOT
    /// parse it; re-derive from the doc.
    #[error("unique index `{index}` on `{collection}` violated by `{key}`")]
    UniqueViolation {
        collection: String,
        index: String,
        key: String,
    },
    /// The target collection of a constrained write is still being
    /// backfilled (create-time scan or touch-load); retry after quiescence
    /// (`await_quiescent` or the readiness signal).
    #[error("index `{index}` on `{collection}` is still backfilling — retry after quiescence")]
    IndexBackfilling {
        collection: String,
        index: String,
    },
    /// A per-collection schema rejected the write (issue #71/F8): a
    /// declared field is missing (and has no default) or has the wrong
    /// declared type. Retrying unchanged input deterministically fails.
    #[error("schema violation on `{collection}.{field}`: expected {expected}")]
    SchemaViolation {
        collection: String,
        field: String,
        expected: String,
    },
}

impl HakoError {
    /// Stable classification for retry policy (Rust) and `hk_error_kind`
    /// (C ABI). Never string-match on the message.
    pub fn kind(&self) -> ErrorKind {
        match self {
            HakoError::Conflict { .. } | HakoError::IndexBackfilling { .. } => {
                ErrorKind::RetryableConflict
            }
            HakoError::UniqueViolation { .. } => ErrorKind::UniqueViolation,
            HakoError::SchemaViolation { .. } => ErrorKind::ConstraintMismatch,
            _ => ErrorKind::Generic,
        }
    }

    /// True iff a retry of identical input can plausibly succeed (normal
    /// backoff; the engine state may have moved on).
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind(), ErrorKind::RetryableConflict)
    }
}

pub type Result<T> = std::result::Result<T, HakoError>;
