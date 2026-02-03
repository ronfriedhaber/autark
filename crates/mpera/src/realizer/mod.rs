use std::collections::HashMap;

use arrow::array::RecordBatch;
use autark_tensor::Tensor;

use crate::{op::OpRef, program::Program};

pub struct Realizer {
    program: Program,
    recordbatches: Vec<RecordBatch>,
    pc: usize,

    series_cache: HashMap<(usize, String), Series>, // TODO: Not vec of vec
}

pub struct Series {
    tensor: Tensor,
}

impl Realizer {
    pub fn new(program: Program, recordbatches: Vec<RecordBatch>) -> Realizer {
        Realizer {
            program,
            recordbatches,
            series_cache: HashMap::new(),
            pc: 0,
        }
    }

    fn process_op(&mut self, ix: usize) {
        let op = self.program.get_op(OpRef(self.pc));
        use crate::op::Op::*;

        // match op {}

        self.pc += 1;
    }
}
