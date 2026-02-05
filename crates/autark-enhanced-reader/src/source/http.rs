use super::Source;
use crate::{Error, Result};
use std::io::{BufRead, BufReader};

pub struct HttpSource;

impl Source for HttpSource {
    fn read(content: &str) -> Result<Box<dyn BufRead + Send>> {
        let response = reqwest::blocking::get(content)
            .map_err(|err| Error::Reader(format!("http error: {err}")))?
            .error_for_status()
            .map_err(|err| Error::Reader(format!("http error: {err}")))?;
        Ok(Box::new(BufReader::new(response)))
    }
}
