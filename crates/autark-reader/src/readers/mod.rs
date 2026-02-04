pub mod csv;
pub mod json;

use crate::Result;
use autark_dataframe::DataFrame;

pub trait OnceReader {
    type Error;

    fn read(self) -> Result<DataFrame>;
}
