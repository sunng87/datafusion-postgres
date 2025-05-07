use std::iter;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset};
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, ParamValues};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use futures::{stream, StreamExt};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{FieldInfo, QueryResponse};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::encoder::row_encoder::RowEncoder;

pub(crate) fn into_pg_type(df_type: &DataType) -> PgWireResult<Type> {
    Ok(match df_type {
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
                DataType::Struct(_) => Type::new(
                    Type::RECORD_ARRAY.name().into(),
                    Type::RECORD_ARRAY.oid(),
                    Type::RECORD_ARRAY.kind().clone(),
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
            Type::new(
                name,
                Type::RECORD.oid(),
                Type::RECORD.kind().clone(),
                Type::RECORD.schema().into(),
            )
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {df_type}"),
            ))));
        }
    })
}

pub(crate) fn df_schema_to_pg_fields(
    schema: &DFSchema,
    format: &Format,
) -> PgWireResult<Vec<FieldInfo>> {
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

pub(crate) async fn encode_dataframe<'a>(
    df: DataFrame,
    format: &Format,
) -> PgWireResult<QueryResponse<'a>> {
    let fields = Arc::new(df_schema_to_pg_fields(df.schema(), format)?);

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            let row_stream: Box<dyn Iterator<Item = PgWireResult<DataRow>> + Send + Sync> = match rb
            {
                Ok(rb) => {
                    let fields = fields_ref.clone();
                    let mut row_stream = RowEncoder::new(rb, fields);
                    Box::new(std::iter::from_fn(move || row_stream.next_row()))
                }
                Err(e) => Box::new(iter::once(Err(PgWireError::ApiError(e.into())))),
            };
            stream::iter(row_stream)
        })
        .flatten();
    Ok(QueryResponse::new(fields, pg_row_stream))
}

/// Deserialize client provided parameter data.
///
/// First we try to use the type information from `pg_type_hint`, which is
/// provided by the client.
/// If the type is empty or unknown, we fallback to datafusion inferenced type
/// from `inferenced_types`.
/// An error will be raised when neither sources can provide type information.
pub(crate) fn deserialize_parameters<S>(
    portal: &Portal<S>,
    inferenced_types: &[Option<&DataType>],
) -> PgWireResult<ParamValues>
where
    S: Clone,
{
    fn get_pg_type(
        pg_type_hint: Option<&Type>,
        inferenced_type: Option<&DataType>,
    ) -> PgWireResult<Type> {
        if let Some(ty) = pg_type_hint {
            Ok(ty.clone())
        } else if let Some(infer_type) = inferenced_type {
            into_pg_type(infer_type)
        } else {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "XX000".to_string(),
                "Unknown parameter type".to_string(),
            ))))
        }
    }

    let param_len = portal.parameter_len();
    let mut deserialized_params = Vec::with_capacity(param_len);
    for i in 0..param_len {
        let pg_type = get_pg_type(
            portal.statement.parameter_types.get(i),
            inferenced_types.get(i).and_then(|v| v.to_owned()),
        )?;
        match pg_type {
            // enumerate all supported parameter types and deserialize the
            // type to ScalarValue
            Type::BOOL => {
                let value = portal.parameter::<bool>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Boolean(value));
            }
            Type::CHAR => {
                let value = portal.parameter::<i8>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int8(value));
            }
            Type::INT2 => {
                let value = portal.parameter::<i16>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int16(value));
            }
            Type::INT4 => {
                let value = portal.parameter::<i32>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int32(value));
            }
            Type::INT8 => {
                let value = portal.parameter::<i64>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int64(value));
            }
            Type::TEXT | Type::VARCHAR => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::BYTEA => {
                let value = portal.parameter::<Vec<u8>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Binary(value));
            }

            Type::FLOAT4 => {
                let value = portal.parameter::<f32>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Float32(value));
            }
            Type::FLOAT8 => {
                let value = portal.parameter::<f64>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Float64(value));
            }
            Type::NUMERIC => {
                let value = match portal.parameter::<Decimal>(i, &pg_type)? {
                    None => ScalarValue::Decimal128(None, 0, 0),
                    Some(value) => {
                        let precision = match value.mantissa() {
                            0 => 1,
                            m => (m.abs() as f64).log10().floor() as u8 + 1,
                        };
                        let scale = value.scale() as i8;
                        ScalarValue::Decimal128(value.to_i128(), precision, scale)
                    }
                };
                deserialized_params.push(value);
            }
            Type::TIMESTAMP => {
                let value = portal.parameter::<NaiveDateTime>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.and_utc().timestamp_micros()),
                    None,
                ));
            }
            Type::TIMESTAMPTZ => {
                let value = portal.parameter::<DateTime<FixedOffset>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.timestamp_micros()),
                    value.map(|t| t.offset().to_string().into()),
                ));
            }
            Type::DATE => {
                let value = portal.parameter::<NaiveDate>(i, &pg_type)?;
                deserialized_params
                    .push(ScalarValue::Date32(value.map(Date32Type::from_naive_date)));
            }
            // TODO: add more types
            _ => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_string(),
                    "XX000".to_string(),
                    format!("Unsupported parameter type: {}", pg_type),
                ))));
            }
        }
    }

    Ok(ParamValues::List(deserialized_params))
}
