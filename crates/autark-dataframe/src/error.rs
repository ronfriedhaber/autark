#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("MPERA Error")]
    MperaError(#[from] mpera::error::Error),

    #[error("TensorError")]
    Tensor(#[from] autark_tensor::error::Error),

    #[error("ArrowError")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("sink error: {0}")]
    Sink(String),
}
