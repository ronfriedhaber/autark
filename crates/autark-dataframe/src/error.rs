#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("MPERA Error")]
    Mpera(#[from] mpera::error::Error),

    #[error("Tensor Error")]
    Tensor(#[from] autark_tensor::error::Error),

    #[error("sink error: {0}")]
    Sink(String),
}
