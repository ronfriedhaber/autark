#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Provided Empty Program")]
    ProvidedEmptyProgram,
    #[error("Problem Interfacing With Error")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Problem Interfacing With Pyo3")]
    PyO3Error(#[from] pyo3::PyErr),
}
