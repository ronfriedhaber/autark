#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Provided Empty Program")]
    ProvidedEmptyProgram,
}
