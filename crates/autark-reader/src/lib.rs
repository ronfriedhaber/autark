pub mod readers;

pub use autark_error::Error;
pub use readers::OnceReader;

pub type Result<T> = std::result::Result<T, Error>;
