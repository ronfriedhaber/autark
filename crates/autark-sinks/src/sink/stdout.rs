use arrow::util::pretty::pretty_format_batches;

use crate::sink::Sink;

pub struct SinkStdout;

impl Sink for SinkStdout {
    type Error = crate::Error;

    // fn init() -> std::result::Result<Self, Self::Error> {
    //     Ok(SinkStdout {})
    // }

    fn sink(&self, output: mpera::output::ProgramOutput) -> std::result::Result<(), Self::Error> {
        for (k, v) in output.0.into_iter() {
            println!(
                r#"
--------  
KEY: {};
VALUE: 
{}
                "#,
                k,
                pretty_format_batches(&[v]).unwrap()
            );

            println!("--------");
        }

        Ok(())
    }

    fn finish(self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}
