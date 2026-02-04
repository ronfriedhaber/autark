use autark_client::{OnceFrame, Result};
use autark_sinks::sink::csv::CsvSink;
use std::{path::PathBuf, str::FromStr};

use arrow::datatypes::{DataType, Field, Schema};
use autark_reader::readers::csv::CsvReader;
use mpera::op::ReduceOpKind;

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
        .alias(
            "age",
            Some(Schema::new(vec![Field::new(
                "age_mean",
                DataType::Float64,
                true,
            )])),
        )?;

    let pclass = frame.p.dataframe(None)?.col("Pclass")?;
    frame
        .p
        .dataframe(None)?
        .col("Fare")?
        .group_by(frame.p.dataframe(None)?.col("Sex")?, ReduceOpKind::Stdev)?
        .alias(
            "fare_by_class",
            Some(Schema::new(vec![
                Field::new("Sex", DataType::Utf8, true),
                Field::new("fare_mean", DataType::Float64, true),
            ])),
        )?;

    // frame.p.dataframe(None)?.slice(0, 10)?.alias(
    //     "frame",
    //     Some(Schema::new(vec![
    //         Field::new("PassengerId", DataType::Float64, true),
    //         Field::new("Survived", DataType::Float64, true),
    //         Field::new("Pclass", DataType::Float64, true),
    //         Field::new("Name", DataType::Float64, true),
    //         Field::new("Sex", DataType::Float64, true),
    //         Field::new("Age", DataType::Float64, true),
    //         Field::new("SibSp", DataType::Float64, true),
    //         Field::new("Parch", DataType::Float64, true),
    //         Field::new("Ticket", DataType::Float64, true),
    //         Field::new("Fare", DataType::Float64, true),
    //         Field::new("Cabin", DataType::Float64, true),
    //         Field::new("Embarked", DataType::Float64, true),
    //     ])),
    // )?;

    frame.realize()?;

    std::thread::sleep_ms(2000);

    Ok(())
}
