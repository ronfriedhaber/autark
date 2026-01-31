#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("MPERA Error")]
    MperaError(mpera::error::Error),
}
