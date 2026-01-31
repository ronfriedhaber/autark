use std::sync::Arc;

use arrow::{
    array::{Array, ArrowPrimitiveType, BooleanArray, PrimitiveArray, RecordBatch, StringArray},
    datatypes::ArrowTimestampType,
};
// use arrow::array::RecordBatch;
use crate::{DataFrame, dtype::DType};

pub type ArrowDataType = arrow::datatypes::DataType;

impl TryFrom<RecordBatch> for DataFrame {
    type Error = crate::Error;

    fn try_from(record_batch: RecordBatch) -> Result<Self, Self::Error> {
        DataFrame::from_internal(record_batch)
    }
}
