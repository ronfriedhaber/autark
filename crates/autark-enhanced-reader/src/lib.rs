mod source;

use std::io::{BufRead, Read};

pub use autark_error::Error;
use source::{HttpSource, LocalSource, Source};
pub type Result<T> = std::result::Result<T, Error>;

pub fn autoread(uri: &str) -> Result<Box<dyn BufRead + Send>> {
    if HttpSource::supports(uri) {
        return HttpSource::read(uri);
    }

    if LocalSource::supports(uri) {
        return LocalSource::read(uri);
    }

    Err(Error::UnsupportedUri(uri.to_string()))
}

// pub fn open_uri(uri: &str) -> Result<Box<dyn BufRead + Send>> {
//     autoread(uri)
// }

pub fn autoread_to_bytes(uri: &str) -> Result<Vec<u8>> {
    let mut reader = autoread(uri)?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;

    Ok(buf)
}
