#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error interfacing with python.")]
    Py(#[from] pyo3::PyErr),

    #[error("Unsupported arrow datatype.")]
    UnsupportedArrowDataType,
}
