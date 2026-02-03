use autark_client::{OnceFrame, Result};
use autark_sinks::{
    sink::csv::CsvSink,
};
use std::{path::PathBuf, str::FromStr};

use autark_reader::readers::csv::CsvReader;
use mpera::{
    op::{BinaryOpKind, ReduceOpKind},
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
