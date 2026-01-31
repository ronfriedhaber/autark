pub(crate) fn debug_level() -> Option<usize> {
    if let Ok(x) = std::env::var("DEBUG") {
        return match x.parse::<usize>() {
            Ok(x) => Some(x),
            Err(_) => None,
        };
    }

    None
}
