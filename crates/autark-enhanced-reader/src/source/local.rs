use super::Source;
use crate::{Error, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub struct LocalSource;

impl Source for LocalSource {
    fn read(content: &str) -> Result<Box<dyn BufRead + Send>> {
        let path = local_path_from_content(content)?;
        let file = File::open(path)?;
        Ok(Box::new(BufReader::new(file)))
    }
}

fn local_path_from_content(content: &str) -> Result<PathBuf> {
    if content.is_empty() {
        return Err(Error::InvalidUri(content.to_string()));
    }

    Ok(PathBuf::from(content))
}
