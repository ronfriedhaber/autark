pub mod artifact;
pub mod codegen;
// pub mod element;
pub mod error;
pub mod op;
pub mod pipeline;
pub mod program;
pub mod programmetadata;
pub mod programpayload;
// pub mod programbuilder;
pub mod output;
pub(crate) mod postprocessing;
pub mod runtime;
pub mod string;
// pub mod tensor;

mod flag;
mod pyfn;
mod with_tinygrad;

pub use crate::artifact::Artifact;
// pub use autark::Tensor;

pub type Result<T> = std::result::Result<T, crate::error::Error>;
