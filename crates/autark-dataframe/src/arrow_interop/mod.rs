
use arrow::array::RecordBatch;
// use arrow::array::RecordBatch;
use crate::DataFrame;

pub type ArrowDataType = arrow::datatypes::DataType;

impl TryFrom<RecordBatch> for DataFrame {
    type Error = crate::Error;

    fn try_from(record_batch: RecordBatch) -> Result<Self, Self::Error> {
        DataFrame::from_internal(record_batch)
    }
}
