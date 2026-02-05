use std::ffi::NulError;
use std::io;

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

    #[error("Reader is void.")]
    EmptyReader,

    #[error("Reader error: {0}")]
    Reader(String),

    #[error("I/O error.")]
    Io(#[from] io::Error),

    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    #[error("Unsupported URI: {0}")]
    UnsupportedUri(String),

    #[error("Nul Error.")]
    NulError(#[from] NulError),

    #[error("Sink problem.")]
    Sink(String),

    #[error("Unsupported Arrow DataType.")]
    UnsupportedArrowDataType, // #[error("Tensor Error.")]
                              // Tensor(#[from] autark_tensor::Error),
}
