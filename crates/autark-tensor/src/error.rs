use pyo3::DowncastError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Problem interoping with python.")]
    Py(#[from] pyo3::PyErr),
    // #[error("Problem interoping with python - downcast.")]
    // Downcast(#[from] Down),
}
