use std::{error::Error, str::FromStr, sync::Arc};

use arrow::{
    datatypes::{
        Date32Type, Date64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
        Time64NanosecondType,
    },
    temporal_conversions::{as_date, as_time},
};
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::{
    array::{
        timezone::Tz, Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        LargeBinaryArray, PrimitiveArray, StringArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{
        DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};
use pgwire::{
    api::results::FieldFormat,
    error::{ErrorInfo, PgWireError},
    types::{ToSqlText, QUOTE_ESCAPE},
};
use postgres_types::{ToSql, Type};
use rust_decimal::Decimal;

use super::{struct_encoder::encode_struct, EncodedValue};

fn get_bool_list_value(arr: &Arc<dyn Array>) -> Vec<Option<bool>> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

macro_rules! get_primitive_list_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>) -> Vec<Option<$pt>> {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .collect()
        }
    };

    ($name:ident, $t:ty, $pt:ty, $f:expr) => {
        fn $name(arr: &Arc<dyn Array>) -> Vec<Option<$pt>> {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .map(|val| val.map($f))
                .collect()
        }
    };
}

get_primitive_list_value!(get_i8_list_value, Int8Type, i8);
get_primitive_list_value!(get_i16_list_value, Int16Type, i16);
get_primitive_list_value!(get_i32_list_value, Int32Type, i32);
get_primitive_list_value!(get_i64_list_value, Int64Type, i64);
get_primitive_list_value!(get_u8_list_value, UInt8Type, i8, |val: u8| { val as i8 });
get_primitive_list_value!(get_u16_list_value, UInt16Type, i16, |val: u16| {
    val as i16
});
get_primitive_list_value!(get_u32_list_value, UInt32Type, u32);
get_primitive_list_value!(get_u64_list_value, UInt64Type, i64, |val: u64| {
    val as i64
});
get_primitive_list_value!(get_f32_list_value, Float32Type, f32);
get_primitive_list_value!(get_f64_list_value, Float64Type, f64);

fn encode_field<T: ToSql + ToSqlText>(
    t: &[T],
    type_: &Type,
    format: FieldFormat,
) -> Result<EncodedValue, Box<dyn Error + Send + Sync>> {
    let mut bytes = BytesMut::new();
    match format {
        FieldFormat::Text => t.to_sql_text(type_, &mut bytes)?,
        FieldFormat::Binary => t.to_sql(type_, &mut bytes)?,
    };
    Ok(EncodedValue { bytes })
}

pub(crate) fn encode_list(
    arr: Arc<dyn Array>,
    type_: &Type,
    format: FieldFormat,
) -> Result<EncodedValue, Box<dyn Error + Send + Sync>> {
    match arr.data_type() {
        DataType::Null => {
            let mut bytes = BytesMut::new();
            match format {
                FieldFormat::Text => None::<i8>.to_sql_text(type_, &mut bytes),
                FieldFormat::Binary => None::<i8>.to_sql(type_, &mut bytes),
            }?;
            Ok(EncodedValue { bytes })
        }
        DataType::Boolean => encode_field(&get_bool_list_value(&arr), type_, format),
        DataType::Int8 => encode_field(&get_i8_list_value(&arr), type_, format),
        DataType::Int16 => encode_field(&get_i16_list_value(&arr), type_, format),
        DataType::Int32 => encode_field(&get_i32_list_value(&arr), type_, format),
        DataType::Int64 => encode_field(&get_i64_list_value(&arr), type_, format),
        DataType::UInt8 => encode_field(&get_u8_list_value(&arr), type_, format),
        DataType::UInt16 => encode_field(&get_u16_list_value(&arr), type_, format),
        DataType::UInt32 => encode_field(&get_u32_list_value(&arr), type_, format),
        DataType::UInt64 => encode_field(&get_u64_list_value(&arr), type_, format),
        DataType::Float32 => encode_field(&get_f32_list_value(&arr), type_, format),
        DataType::Float64 => encode_field(&get_f64_list_value(&arr), type_, format),
        DataType::Decimal128(_, s) => {
            let value: Vec<_> = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .map(|ov| ov.map(|v| Decimal::from_i128_with_scale(v, *s as u32)))
                .collect();
            encode_field(&value, type_, format)
        }
        DataType::Utf8 => {
            let value: Vec<Option<&str>> = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect();
            encode_field(&value, type_, format)
        }
        DataType::Binary => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .iter()
                .collect();
            encode_field(&value, type_, format)
        }
        DataType::LargeBinary => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .iter()
                .collect();
            encode_field(&value, type_, format)
        }

        DataType::Date32 => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .iter()
                .map(|val| val.and_then(|x| as_date::<Date32Type>(x as i64)))
                .collect();
            encode_field(&value, type_, format)
        }
        DataType::Date64 => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .iter()
                .map(|val| val.and_then(as_date::<Date64Type>))
                .collect();
            encode_field(&value, type_, format)
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(|x| as_time::<Time32SecondType>(x as i64)))
                    .collect();
                encode_field(&value, type_, format)
            }
            TimeUnit::Millisecond => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(|x| as_time::<Time32MillisecondType>(x as i64)))
                    .collect();
                encode_field(&value, type_, format)
            }
            _ => {
                unimplemented!()
            }
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(as_time::<Time64MicrosecondType>))
                    .collect();
                encode_field(&value, type_, format)
            }
            TimeUnit::Nanosecond => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(as_time::<Time64NanosecondType>))
                    .collect();
                encode_field(&value, type_, format)
            }
            _ => {
                unimplemented!()
            }
        },
        DataType::Timestamp(unit, timezone) => match unit {
            TimeUnit::Second => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref())
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp(i, 0).map(|dt| {
                                    Utc.from_utc_datetime(&dt.naive_utc())
                                        .with_timezone(&tz)
                                        .fixed_offset()
                                })
                            })
                        })
                        .collect();
                    encode_field(&value, type_, format)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| DateTime::from_timestamp(i, 0).map(|dt| dt.naive_utc()))
                        })
                        .collect();
                    encode_field(&value, type_, format)
                }
            }
            TimeUnit::Millisecond => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref())
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_millis(i).map(|dt| {
                                    Utc.from_utc_datetime(&dt.naive_utc())
                                        .with_timezone(&tz)
                                        .fixed_offset()
                                })
                            })
                        })
                        .collect();
                    encode_field(&value, type_, format)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_millis(i).map(|dt| dt.naive_utc())
                            })
                        })
                        .collect();
                    encode_field(&value, type_, format)
                }
            }
            TimeUnit::Microsecond => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref())
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_micros(i).map(|dt| {
                                    Utc.from_utc_datetime(&dt.naive_utc())
                                        .with_timezone(&tz)
                                        .fixed_offset()
                                })
                            })
                        })
                        .collect();
                    encode_field(&value, type_, format)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_micros(i).map(|dt| dt.naive_utc())
                            })
                        })
                        .collect();
                    encode_field(&value, type_, format)
                }
            }
            TimeUnit::Nanosecond => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref())
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.map(|i| {
                                Utc.from_utc_datetime(
                                    &DateTime::from_timestamp_nanos(i).naive_utc(),
                                )
                                .with_timezone(&tz)
                                .fixed_offset()
                            })
                        })
                        .collect();
                    encode_field(&value, type_, format)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| i.map(|i| DateTime::from_timestamp_nanos(i).naive_utc()))
                        .collect();
                    encode_field(&value, type_, format)
                }
            }
        },
        DataType::Struct(_) => {
            let fields = match type_.kind() {
                postgres_types::Kind::Array(struct_type_) => Ok(struct_type_),
                _ => Err(format!(
                    "Expected list type found type {} of kind {:?}",
                    type_,
                    type_.kind()
                )),
            }
            .and_then(|struct_type| match struct_type.kind() {
                postgres_types::Kind::Composite(fields) => Ok(fields),
                _ => Err(format!(
                    "Failed to unwrap a composite type inside from type {} kind {:?}",
                    type_,
                    type_.kind()
                )),
            })
            .map_err(|err| {
                let err = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), err);
                Box::new(PgWireError::UserError(Box::new(err)))
            })?;

            let values: Result<Vec<_>, _> = (0..arr.len())
                .map(|row| encode_struct(&arr, row, fields, format))
                .map(|x| {
                    if matches!(format, FieldFormat::Text) {
                        x.map(|opt| {
                            opt.map(|value| {
                                let mut w = BytesMut::new();
                                w.put_u8(b'"');
                                w.put_slice(
                                    QUOTE_ESCAPE
                                        .replace_all(
                                            &String::from_utf8_lossy(&value.bytes),
                                            r#"\$1"#,
                                        )
                                        .as_bytes(),
                                );
                                w.put_u8(b'"');
                                EncodedValue { bytes: w }
                            })
                        })
                    } else {
                        x
                    }
                })
                .collect();
            encode_field(&values?, type_, format)
        }
        // TODO: more types
        list_type => {
            let err = PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "Unsupported List Datatype {} and array {:?}",
                    list_type, &arr
                ),
            )));

            Err(Box::new(err))
        }
    }
}
