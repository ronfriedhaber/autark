pub mod onceframe;

pub use autark_error::Error;
pub use onceframe::OnceFrame;

pub type Result<T> = std::result::Result<T, Error>;
