pub mod csv;
pub mod json;

use autark_dataframe::DataFrame;
use crate::Result;

pub trait OnceReader {
    type Error;

    fn read(self) -> Result<DataFrame>;
}
