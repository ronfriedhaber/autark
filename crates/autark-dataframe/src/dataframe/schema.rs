use std::collections::HashMap;

use crate::dtype::DType;

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<(String, DType)>,
}

impl Schema {
    pub fn new(columns: Vec<(String, DType)>) -> Schema {
        Schema { columns }
    }

    pub fn names(&self) -> Vec<String> {
        self.columns.iter().map(|(name, _)| name.clone()).collect()
    }

    pub(crate) fn concat(a: Schema, b: Self) -> Self {
        let columns = [a.columns, b.columns].concat();
        Schema::new(columns)
    }

    pub(crate) fn name2index(&self) -> HashMap<String, usize> {
        self.columns
            .iter()
            .enumerate()
            .map(|(ix, (name, _))| (name.clone(), ix))
            .collect()
    }
}
