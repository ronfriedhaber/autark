use std::collections::{HashMap, HashSet};

use arrow::array::RecordBatch;

// all pub at least for now
#[derive(Debug, Clone)]
pub struct ProgramOutput(pub HashMap<String, RecordBatch>);

// TODO: Actually impl, contemporary output format is in some sense horrendoues, way too many allocs.
pub fn fuse(xs: &[ProgramOutput]) -> ProgramOutput {
    //     let mut out: HashMap<String, Vec<RecordBatch>> = HashMap::with_capacity(1024);

    //     for i in xs {
    //         // TODO: validate equal keys
    //         // let keys = i.0.keys();
    //         //
    //         for (k, v) in i.0.into_iter() {
    //             out.insert(k, v.clone()); // TODO: no clone!!
    //         }
    //     }

    //     let out HashMap<String, RecordBatch>

    (&xs[0]).clone()
}
