use arrow::array::RecordBatch;

use crate::program::Program;

pub struct Realizer {
    program: Program,
    recordbatches: Vec<RecordBatch>,
}

impl Realizer {
    pub fn new(program: Program, recordbatches: Vec<RecordBatch>) -> Realizer {
        Realizer {
            program,
            recordbatches,
        }
    }

    // pub fn next()
}
