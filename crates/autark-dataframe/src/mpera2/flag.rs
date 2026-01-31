pub(crate) fn debug_level() -> Option<DebugLevel> {
    if let Ok(x) = std::env::var("MPERA_DEBUG") {
        return match x.parse::<u8>() {
            Ok(x) => match x {
                2 => Some(DebugLevel::II),
                _ => todo!(),
            },
            Err(_) => None,
        };
    }

    None
}

#[derive(Debug, PartialEq, Eq)]
pub enum DebugLevel {
    II,
}
