pub mod csv;
pub mod json;

use crate::{DataFrame, Result};

pub trait Reader {
    type Error;

    fn next(&mut self) -> Result<Option<DataFrame>>;
}
