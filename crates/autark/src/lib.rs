use autark_error::Error;

pub mod onceframe;
pub mod prelude;
pub mod mpera {
    pub use mpera::*;
}

// pub use autark_error::Error;
// pub use onceframe::OnceFrame;
// pub use onceframe::;

pub(crate) type Result<T> = std::result::Result<T, Error>;
