use std::sync::Arc;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use postgres_types::Kind;

use crate::row_encoder::RowEncoder;

pub fn into_pg_type(arrow_type: &DataType) -> PgWireResult<Type> {
    Ok(match arrow_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::UInt8 => Type::CHAR,
        DataType::Int16 | DataType::UInt16 => Type::INT2,
        DataType::Int32 | DataType::UInt32 => Type::INT4,
        DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                Type::TIMESTAMPTZ
            } else {
                Type::TIMESTAMP
            }
        }
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Interval(_) => Type::INTERVAL,
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => Type::BYTEA,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        DataType::Utf8 => Type::VARCHAR,
        DataType::LargeUtf8 => Type::TEXT,
        DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::LargeList(field) => {
            match field.data_type() {
                DataType::Boolean => Type::BOOL_ARRAY,
                DataType::Int8 | DataType::UInt8 => Type::CHAR_ARRAY,
                DataType::Int16 | DataType::UInt16 => Type::INT2_ARRAY,
                DataType::Int32 | DataType::UInt32 => Type::INT4_ARRAY,
                DataType::Int64 | DataType::UInt64 => Type::INT8_ARRAY,
                DataType::Timestamp(_, tz) => {
                    if tz.is_some() {
                        Type::TIMESTAMPTZ_ARRAY
                    } else {
                        Type::TIMESTAMP_ARRAY
                    }
                }
                DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
                DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
                DataType::Interval(_) => Type::INTERVAL_ARRAY,
                DataType::FixedSizeBinary(_) | DataType::Binary => Type::BYTEA_ARRAY,
                DataType::Float16 | DataType::Float32 => Type::FLOAT4_ARRAY,
                DataType::Float64 => Type::FLOAT8_ARRAY,
                DataType::Utf8 => Type::VARCHAR_ARRAY,
                DataType::LargeUtf8 => Type::TEXT_ARRAY,
                struct_type @ DataType::Struct(_) => Type::new(
                    Type::RECORD_ARRAY.name().into(),
                    Type::RECORD_ARRAY.oid(),
                    Kind::Array(into_pg_type(struct_type)?),
                    Type::RECORD_ARRAY.schema().into(),
                ),
                list_type => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("Unsupported List Datatype {list_type}"),
                    ))));
                }
            }
        }
        DataType::Utf8View => Type::TEXT,
        DataType::Dictionary(_, value_type) => into_pg_type(value_type)?,
        DataType::Struct(fields) => {
            let name: String = fields
                .iter()
                .map(|x| x.name().clone())
                .reduce(|a, b| a + ", " + &b)
                .map(|x| format!("({x})"))
                .unwrap_or("()".to_string());
            let kind = Kind::Composite(
                fields
                    .iter()
                    .map(|x| {
                        into_pg_type(x.data_type())
                            .map(|_type| postgres_types::Field::new(x.name().clone(), _type))
                    })
                    .collect::<Result<Vec<_>, PgWireError>>()?,
            );
            Type::new(name, Type::RECORD.oid(), kind, Type::RECORD.schema().into())
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {arrow_type}"),
            ))));
        }
    })
}

pub fn arrow_schema_to_pg_fields(schema: &Schema, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let pg_type = into_pg_type(f.data_type())?;
            Ok(FieldInfo::new(
                f.name().into(),
                None,
                None,
                pg_type,
                format.format_for(idx),
            ))
        })
        .collect::<PgWireResult<Vec<FieldInfo>>>()
}

pub fn encode_recordbatch(
    fields: Arc<Vec<FieldInfo>>,
    record_batch: RecordBatch,
) -> Box<impl Iterator<Item = PgWireResult<DataRow>>> {
    let mut row_stream = RowEncoder::new(record_batch, fields);
    Box::new(std::iter::from_fn(move || row_stream.next_row()))
}
