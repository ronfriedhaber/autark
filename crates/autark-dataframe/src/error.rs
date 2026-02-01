#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("MPERA Error")]
    MperaError(#[source] mpera::error::Error),

    #[error("sink error: {0}")]
    Sink(String),
}
