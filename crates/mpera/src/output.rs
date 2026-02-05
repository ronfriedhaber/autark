use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use arrow::{array::RecordBatch, ipc::writer::StreamWriter};

// all pub at least for now
#[derive(Debug, Clone)]
pub struct ProgramOutput(pub HashMap<String, RecordBatch>);

impl Hash for ProgramOutput {
    fn hash<H: Hasher>(&self, s: &mut H) {
        let mut ks: Vec<_> = self.0.keys().collect();
        ks.sort();
        ks.len().hash(s);

        ks.into_iter().for_each(|k| {
            k.hash(s);
            let mut buf = Vec::new();
            let b = &self.0[k];
            StreamWriter::try_new(&mut buf, &b.schema())
                .unwrap()
                .write(b)
                .unwrap();
            buf.hash(s);
        });
    }
}

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
