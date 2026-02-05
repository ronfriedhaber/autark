mod medium;
mod parse;
mod source;

use std::io::{BufRead, Read};

pub use autark_error::Error;
pub use medium::Medium;
use source::{HttpSource, LocalSource, Source};
pub type Result<T> = std::result::Result<T, Error>;

pub fn autoread(uri: &str) -> Result<Box<dyn BufRead + Send>> {
    let (medium, content) = parse::parse(uri)?;
    match medium {
        Medium::Http => HttpSource::read(&content),
        Medium::Local => LocalSource::read(&content),
    }
}

pub fn open_uri(uri: &str) -> Result<Box<dyn BufRead + Send>> {
    autoread(uri)
}

pub fn open_uri_bytes(uri: &str) -> Result<Vec<u8>> {
    let mut reader = open_uri(uri)?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;

    Ok(buf)
}

pub fn autoread_to_bytes(uri: &str) -> Result<Vec<u8>> {
    open_uri_bytes(uri)
}
