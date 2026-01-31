use crate::sink::Sink;

pub struct SinkVoid {}

impl Sink for SinkVoid {
    type Error = crate::Error;

    // fn init() -> std::result::Result<Self, Self::Error> {
    //     Ok(SinkVoid {})
    // }
    fn finish(self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn sink(&self, _output: mpera::output::ProgramOutput) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}
