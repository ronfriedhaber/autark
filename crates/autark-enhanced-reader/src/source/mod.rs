mod http;
mod local;

use crate::Result;
use std::io::BufRead;

pub use http::HttpSource;
pub use local::LocalSource;

pub trait Source: Send + Sync {
    fn supports(uri: &str) -> bool;
    fn read(uri: &str) -> Result<Box<dyn BufRead + Send>>;
}
