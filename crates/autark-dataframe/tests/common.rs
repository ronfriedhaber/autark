use std::process::Command;

pub fn hash_of_dir(dir: &str) -> std::io::Result<String> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "find '{}' -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum",
            dir
        ))
        .output()?;

    if !output.status.success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "command failed",
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap()
        .to_string())
}
