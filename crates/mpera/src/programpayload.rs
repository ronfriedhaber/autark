use crate::dataadapter::DataFramePayload;

#[derive(Debug, Clone)]
pub struct ProgramPayload {
    pub(crate) dataframes: Vec<DataFramePayload>,
}

impl ProgramPayload {
    pub fn new(dataframes: Vec<DataFramePayload>) -> ProgramPayload {
        ProgramPayload { dataframes }
    }
}
