use super::Source;
use crate::{Error, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub struct LocalSource;

impl Source for LocalSource {
    fn supports(uri: &str) -> bool {
        uri.starts_with("file://") || !uri.contains("://")
    }

    fn read(uri: &str) -> Result<Box<dyn BufRead + Send>> {
        let path = local_path_from_uri(uri)?;
        let file = File::open(path)?;
        Ok(Box::new(BufReader::new(file)))
    }
}

fn local_path_from_uri(uri: &str) -> Result<PathBuf> {
    if uri.starts_with("file://") {
        let path = uri.trim_start_matches("file://");
        if path.is_empty() {
            return Err(Error::InvalidUri(uri.to_string()));
        }
        return Ok(PathBuf::from(path));
    }

    Ok(PathBuf::from(uri))
}
