use autark_client::{OnceFrame, Result};
use autark_sinks::sink::csv::CsvSink;
use std::{path::PathBuf, str::FromStr, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use autark_reader::readers::csv::CsvReader;
use mpera::op::{JoinKind, ReduceOpKind};

mod common;

#[test]
fn t1() -> Result<()> {
    let csv_reader =
        CsvReader::new(PathBuf::from_str("../../extra/datasets/titanic_train_0.csv").unwrap())
            .unwrap();

    let csv_reader_2 =
        CsvReader::new(PathBuf::from_str("../../extra/datasets/titanic_train_0.csv").unwrap())
            .unwrap();
    let frame = OnceFrame::new(
        csv_reader,
        CsvSink::new(PathBuf::from_str("./tmp").unwrap()).unwrap(),
    )
    .with_reader(csv_reader_2);
    frame
        .p
        .dataframe(None)?
        .col("Age")?
        .reduce(ReduceOpKind::Mean)?
        .alias(
            "age",
            Some(Schema::new(vec![Arc::new(Field::new(
                "age_mean",
                DataType::Float64,
                true,
            ))])),
        )?;

    let _pclass = frame.p.dataframe(None)?.col("Pclass")?;
    frame
        .p
        .dataframe(None)?
        .col("Fare")?
        .group_by(frame.p.dataframe(None)?.col("Sex")?, ReduceOpKind::Stdev)?
        .alias(
            "fare_by_class",
            Some(Schema::new(vec![
                frame
                    .schema_of_columns(None, &["Sex"])?
                    .fields()[0]
                    .clone(),
                Arc::new(Field::new("fare_mean", DataType::Float64, true)),
            ])),
        )?;
    dbg!(frame.schema_of_columns(None, &["Sex"]));
    frame
        .p
        .dataframe(None)?
        .col("Sex")?
        .slice(0, 10)?
        .alias(
            "sliced_gender",
            Some(frame.schema_of_columns(None, &["Sex"])?),
        )?;

    let left = frame.p.dataframe(Some(0))?;
    let right = frame.p.dataframe(Some(1))?;
    let left_key = left.col("PassengerId")?;
    let right_key = right.col("PassengerId")?;
    let base_schema = frame.schema(Some(0))?;
    let mut join_fields = Vec::with_capacity(base_schema.fields().len() * 2);
    join_fields.extend(base_schema.fields().iter().cloned());
    join_fields.extend(base_schema.fields().iter().cloned());
    left.join(right, left_key, right_key, JoinKind::LeftOuter)?
        .alias("joined", Some(Schema::new(join_fields)))?;

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
