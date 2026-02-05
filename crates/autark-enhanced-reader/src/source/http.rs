use super::Source;
use crate::{Error, Result};
use std::io::{BufRead, BufReader};

pub struct HttpSource;

impl Source for HttpSource {
    fn supports(uri: &str) -> bool {
        uri.starts_with("http://") || uri.starts_with("https://")
    }

    fn read(uri: &str) -> Result<Box<dyn BufRead + Send>> {
        let response = reqwest::blocking::get(uri)
            .map_err(|err| Error::Reader(format!("http error: {err}")))?
            .error_for_status()
            .map_err(|err| Error::Reader(format!("http error: {err}")))?;
        Ok(Box::new(BufReader::new(response)))
    }
}
