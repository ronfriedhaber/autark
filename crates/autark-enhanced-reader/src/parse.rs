use crate::{Error, Medium, Result};

pub fn parse(uri: &str) -> Result<(Medium, String)> {
    if uri.is_empty() {
        return Err(Error::InvalidUri(uri.to_string()));
    }

    if uri.starts_with("http://") || uri.starts_with("https://") {
        return Ok((Medium::Http, uri.to_string()));
    }

    if uri.starts_with("file://") {
        let path = uri.trim_start_matches("file://");
        if path.is_empty() {
            return Err(Error::InvalidUri(uri.to_string()));
        }
        return Ok((Medium::Local, path.to_string()));
    }

    if uri.contains("://") {
        return Err(Error::UnsupportedUri(uri.to_string()));
    }

    Ok((Medium::Local, uri.to_string()))
}
