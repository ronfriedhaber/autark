pub mod csv;
pub mod json;

use crate::Result;
use arrow::datatypes::Schema;
use autark_dataframe::DataFrame;
use std::sync::Arc;

pub trait OnceReader {
    fn read(&mut self) -> Result<DataFrame>;
    fn schema(&self) -> Result<Arc<Schema>>;
}
