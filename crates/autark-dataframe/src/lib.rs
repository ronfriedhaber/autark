#![deny(clippy::unwrap_used)]

pub(crate) mod arrow_interop;
pub(crate) mod dataframe;
pub(crate) mod error;
pub mod onceframe;
pub mod readers;
pub mod sink;
pub(crate) mod slimframe;

pub use dataframe::*;
pub use error::*;

pub use autark_tensor::Tensor;
pub use mpera::program::Program;

pub type Result<T> = std::result::Result<T, Error>;
