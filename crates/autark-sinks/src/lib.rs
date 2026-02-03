pub mod sink;

pub use autark_error::Error;
pub use sink::Sink;

pub type Result<T> = std::result::Result<T, Error>;
