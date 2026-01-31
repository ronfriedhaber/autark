use crate::arrow_interop::ArrowDataType;

#[derive(Debug, Clone, Copy)]
pub enum DType {
    F64,
    String,
}

impl Into<ArrowDataType> for DType {
    fn into(self) -> ArrowDataType {
        use DType::*;
        match self {
            F64 => ArrowDataType::Float32,
            String => ArrowDataType::Utf8,
        }
    }
}
