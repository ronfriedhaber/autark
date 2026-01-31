pub mod artifact;
pub mod codegen;
pub mod dataadapter;
// pub mod element;
pub mod error;
pub mod op;
pub mod pipeline;
pub mod program;
// pub mod programbuilder;
pub mod runtime;
pub mod tensor;

mod flag;
mod pyfn;
mod with_tinygrad;

pub use crate::artifact::Artifact;
pub use crate::tensor::Tensor;

pub type Result<T> = std::result::Result<T, crate::error::Error>;
