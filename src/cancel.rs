use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{HakoError, Result};

/// Cheap-to-clone cancellation flag for long-running operations.
///
/// Pass one to the `*_with_cancel` variants (`compact`, `vacuum`,
/// `delete_where`, `patch_where`). Calling [`Self::cancel`] asks the
/// operation to stop at its next safe checkpoint; a clone observes the
/// same flag.
///
/// Cancellation is cooperative: an operation may still finish work that
/// was already in flight. For bulk mutations, batches committed before
/// cancellation remain committed.
#[derive(Debug, Clone, Default)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Idempotent and safe from any thread.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Return [`HakoError::Cancelled`] once cancellation was requested.
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(HakoError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn clones_share_one_flag() {
        let token = CancelToken::new();
        let clone = token.clone();
        assert!(!clone.is_cancelled());
        token.cancel();
        assert!(clone.is_cancelled());
        assert!(clone.check().is_err());
    }

    #[test]
    fn cancelled_error_is_classified() {
        let token = CancelToken::new();
        token.cancel();
        let err = token.check().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert!(!err.is_retryable());
    }
}
