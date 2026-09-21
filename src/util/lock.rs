use std::sync::{RwLock, Mutex, RwLockReadGuard, RwLockWriteGuard, MutexGuard};
use crate::error::{HakoError, Result};

pub trait SafeLock<T> {
    fn safe_read(&self) -> Result<RwLockReadGuard<'_, T>>;
    fn safe_write(&self) -> Result<RwLockWriteGuard<'_, T>>;
}

impl<T> SafeLock<T> for RwLock<T> {
    fn safe_read(&self) -> Result<RwLockReadGuard<'_, T>> {
        self.read().map_err(|_| HakoError::LockPoisoned("RwLock read poisoned".into()))
    }
    fn safe_write(&self) -> Result<RwLockWriteGuard<'_, T>> {
        self.write().map_err(|_| HakoError::LockPoisoned("RwLock write poisoned".into()))
    }
}

pub trait SafeMutex<T> {
    fn safe_lock(&self) -> Result<MutexGuard<'_, T>>;
}

impl<T> SafeMutex<T> for Mutex<T> {
    fn safe_lock(&self) -> Result<MutexGuard<'_, T>> {
        self.lock().map_err(|_| HakoError::LockPoisoned("Mutex poisoned".into()))
    }
}