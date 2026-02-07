pub use crate::mpera::op::ReduceKind;
pub use crate::onceframe::OnceFrame;
pub use autark_error::Error;
pub use autark_reader::readers;
pub use autark_sinks::sink;

pub type AutarkResult<T> = std::result::Result<T, Error>;
