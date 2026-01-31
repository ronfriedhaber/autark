pub mod csv;

use crate::{DataFrame, Result};

pub trait Reader {
    type Error;

    fn next(&mut self) -> Result<Option<DataFrame>>;
}
