use arrow::{array::RecordBatch, datatypes::Schema};
use autark_dataframe::DataFrame;

use crate::OnceReader;

pub struct ArrowReader {
    rb: RecordBatch,
}

impl ArrowReader {
    pub fn new(rb: RecordBatch) -> ArrowReader {
        ArrowReader { rb }
    }
}

impl OnceReader for ArrowReader {
    fn read(&mut self) -> crate::Result<autark_dataframe::DataFrame> {
        // TODO: remove clone.
        Ok(DataFrame::try_from(self.rb.clone())?)
    }

    fn schema(&self) -> crate::Result<std::sync::Arc<Schema>> {
        Ok(self.rb.schema())
    }
}
