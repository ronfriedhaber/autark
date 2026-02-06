pub use crate::onceframe::OnceFrame;
pub use autark_error::Error;
pub use autark_reader::readers;
pub use autark_sinks::sink;
pub use mpera::op::ReduceKind;

pub type AutarkResult<T> = std::result::Result<T, Error>;
