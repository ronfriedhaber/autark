use std::collections::HashMap;

use arrow::datatypes::Schema;

#[derive(Debug, Clone, Default)]
pub struct ProgramMetadata {
    pub schema_map: HashMap<String, Schema>,
}
