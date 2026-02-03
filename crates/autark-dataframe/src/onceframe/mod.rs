use mpera::{
    output::{ProgramOutput, fuse},
    pipeline::Pipeline,
    program::Program,
    programpayload::ProgramPayload,
    runtime::Runtime,
};

use crate::{Error, Result, readers::OnceReader, sink::Sink};

pub struct OnceFrame<R: OnceReader, S: Sink> {
    reader: R,
    sink: S,
    pub p: Program,
}

impl<R: OnceReader, S: Sink> OnceFrame<R, S> {
    pub fn new(reader: R, sink: S) -> OnceFrame<R, S> {
        OnceFrame {
            reader,
            sink,
            p: Program::new(),
        }
    }

    // shall take S: Sink
    // thinking: what happens if needers more than one frame, perchance acept sequence of reader
    pub fn realize(self) -> Result<()> {
        let pipeline = Pipeline::new(self.p);
        let artifact = pipeline.run()?;
        let runtime = Runtime::new(artifact);

        let df = self.reader.read()?;
        let output = runtime.run(ProgramPayload::new(vec![df.into()])?)?;
        let outputs = vec![output];

        self.sink
            .sink(fuse(&outputs))
            .map_err(|err| Error::Sink(err.to_string()))?;

        Ok(())
    }
}
