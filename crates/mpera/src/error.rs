use std::{ffi::NulError, sync::PoisonError};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Provided Empty Program")]
    ProvidedEmptyProgram,
    #[error("Problem initializing program.")]
    ErrorInitializingProgram,

    #[error("Problem interfacing with CPython.")]
    PyO3Error(#[from] pyo3::PyErr),

    #[error("Problem interfacing with Arrow.")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Poisoned lock.")]
    PoisonedLock,

    #[error("Nul Error.")]
    NulError(#[from] NulError),

    #[error("Tensor Error.")]
    Tensor(#[from] autark_tensor::Error),
}
