use arrow::util::pretty::print_batches;
use autark_dataframe::{
    Program, Result,
    onceframe::OnceFrame,
    sink::{Sink, csv::CsvSink, stdout::SinkStdout, void::SinkVoid},
};
use std::{path::PathBuf, str::FromStr};

use autark_dataframe::{
    DataFrame,
    readers::{Reader, csv::CsvReader},
};
use mpera::{
    op::{BinaryOpKind, ReduceOpKind},
    pipeline::Pipeline,
    runtime::Runtime,
};

use crate::common::hash_of_dir;

mod common;

#[test]
fn t1() -> Result<()> {
    let csv_reader =
        CsvReader::new(PathBuf::from_str("../../extra/datasets/titanic_train_0.csv").unwrap())
            .unwrap();

    let frame = OnceFrame::new(
        csv_reader,
        CsvSink::new(PathBuf::from_str("./tmp").unwrap()).unwrap(),
    );
    frame
        .p
        .dataframe(None)?
        .col("Age")?
        .reduce(ReduceOpKind::Mean)?
        .alias("age", None)?;

    frame
        .p
        .dataframe(None)?
        .slice(0, 10)?
        .alias("frame", None)?;

    frame.realize()?;

    std::thread::sleep_ms(2000);

    Ok(())
}
