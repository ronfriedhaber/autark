pub mod csv;
pub mod json;

use crate::{DataFrame, Result};

pub trait OnceReader {
    type Error;

    fn read(self) -> Result<DataFrame>;
}
