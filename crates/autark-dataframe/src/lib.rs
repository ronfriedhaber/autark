#![deny(clippy::unwrap_used)]

pub(crate) mod arrow_interop;
pub(crate) mod dataadapter;
pub(crate) mod dataframe;
pub(crate) mod error;

pub use dataadapter::DataFramePayload;
pub use dataframe::*;
pub use error::*;

pub use autark_tensor::Tensor;

pub type Result<T> = std::result::Result<T, Error>;
