use std::error::Error;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use bytes::BufMut;
use bytes::BytesMut;
use chrono::{NaiveDate, NaiveDateTime};
use pgwire::api::results::DataRowEncoder;
use pgwire::api::results::FieldFormat;
use pgwire::error::PgWireError;
use pgwire::error::PgWireResult;
use pgwire::types::ToSqlText;
use postgres_types::{ToSql, Type};
use rust_decimal::Decimal;
use timezone::Tz;

use crate::error::ToSqlError;
use crate::list_encoder::encode_list;
use crate::struct_encoder::encode_struct;

pub trait Encoder {
    fn encode_field_with_type_and_format<T>(
        &mut self,
        value: &T,
        data_type: &Type,
        format: FieldFormat,
    ) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized;
}

impl Encoder for DataRowEncoder {
    fn encode_field_with_type_and_format<T>(
        &mut self,
        value: &T,
        data_type: &Type,
        format: FieldFormat,
    ) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        self.encode_field_with_type_and_format(value, data_type, format)
    }
}

pub(crate) struct EncodedValue {
    pub(crate) bytes: BytesMut,
}

impl std::fmt::Debug for EncodedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedValue").finish()
    }
}

impl ToSql for EncodedValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        out.writer().write_all(&self.bytes)?;
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Send + Sync>> {
        self.to_sql(ty, out)
    }
}

impl ToSqlText for EncodedValue {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        out.writer().write_all(&self.bytes)?;
        Ok(postgres_types::IsNull::No)
    }
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> Option<bool> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(idx)
    })
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Option<$pt> {
            (!arr.is_null(idx)).then(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<$t>>()
                    .unwrap()
                    .value(idx)
            })
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
get_primitive_value!(get_u8_value, UInt8Type, u8);
get_primitive_value!(get_u16_value, UInt16Type, u16);
get_primitive_value!(get_u32_value, UInt32Type, u32);
get_primitive_value!(get_u64_value, UInt64Type, u64);
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);

fn get_utf8_view_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_large_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_binary_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_large_binary_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_date32_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .value_as_date(idx)
}

fn get_date64_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Date64Array>()
        .unwrap()
        .value_as_date(idx)
}

fn get_time32_second_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_time32_millisecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_time64_microsecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}
fn get_time64_nanosecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_numeric_128_value(
    arr: &Arc<dyn Array>,
    idx: usize,
    scale: u32,
) -> PgWireResult<Option<Decimal>> {
    if arr.is_null(idx) {
        return Ok(None);
    }

    let array = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let value = array.value(idx);
    Decimal::try_from_i128_with_scale(value, scale)
        .map_err(|e| {
            let message = match e {
                rust_decimal::Error::ExceedsMaximumPossibleValue => {
                    "Exceeds maximum possible value"
                }
                rust_decimal::Error::LessThanMinimumPossibleValue => {
                    "Less than minimum possible value"
                }
                rust_decimal::Error::ScaleExceedsMaximumPrecision(_) => {
                    "Scale exceeds maximum precision"
                }
                _ => unreachable!(),
            };
            // TODO: add error type in PgWireError
            PgWireError::ApiError(ToSqlError::from(message))
        })
        .map(Some)
}

pub fn encode_value<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    type_: &Type,
    format: FieldFormat,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Null => encoder.encode_field_with_type_and_format(&None::<i8>, type_, format)?,
        DataType::Boolean => {
            encoder.encode_field_with_type_and_format(&get_bool_value(arr, idx), type_, format)?
        }
        DataType::Int8 => {
            encoder.encode_field_with_type_and_format(&get_i8_value(arr, idx), type_, format)?
        }
        DataType::Int16 => {
            encoder.encode_field_with_type_and_format(&get_i16_value(arr, idx), type_, format)?
        }
        DataType::Int32 => {
            encoder.encode_field_with_type_and_format(&get_i32_value(arr, idx), type_, format)?
        }
        DataType::Int64 => {
            encoder.encode_field_with_type_and_format(&get_i64_value(arr, idx), type_, format)?
        }
        DataType::UInt8 => encoder.encode_field_with_type_and_format(
            &(get_u8_value(arr, idx).map(|x| x as i8)),
            type_,
            format,
        )?,
        DataType::UInt16 => encoder.encode_field_with_type_and_format(
            &(get_u16_value(arr, idx).map(|x| x as i16)),
            type_,
            format,
        )?,
        DataType::UInt32 => {
            encoder.encode_field_with_type_and_format(&get_u32_value(arr, idx), type_, format)?
        }
        DataType::UInt64 => encoder.encode_field_with_type_and_format(
            &(get_u64_value(arr, idx).map(|x| x as i64)),
            type_,
            format,
        )?,
        DataType::Float32 => {
            encoder.encode_field_with_type_and_format(&get_f32_value(arr, idx), type_, format)?
        }
        DataType::Float64 => {
            encoder.encode_field_with_type_and_format(&get_f64_value(arr, idx), type_, format)?
        }
        DataType::Decimal128(_, s) => encoder.encode_field_with_type_and_format(
            &get_numeric_128_value(arr, idx, *s as u32)?,
            type_,
            format,
        )?,
        DataType::Utf8 => {
            encoder.encode_field_with_type_and_format(&get_utf8_value(arr, idx), type_, format)?
        }
        DataType::Utf8View => encoder.encode_field_with_type_and_format(
            &get_utf8_view_value(arr, idx),
            type_,
            format,
        )?,
        DataType::LargeUtf8 => encoder.encode_field_with_type_and_format(
            &get_large_utf8_value(arr, idx),
            type_,
            format,
        )?,
        DataType::Binary => {
            encoder.encode_field_with_type_and_format(&get_binary_value(arr, idx), type_, format)?
        }
        DataType::LargeBinary => encoder.encode_field_with_type_and_format(
            &get_large_binary_value(arr, idx),
            type_,
            format,
        )?,
        DataType::Date32 => {
            encoder.encode_field_with_type_and_format(&get_date32_value(arr, idx), type_, format)?
        }
        DataType::Date64 => {
            encoder.encode_field_with_type_and_format(&get_date64_value(arr, idx), type_, format)?
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => encoder.encode_field_with_type_and_format(
                &get_time32_second_value(arr, idx),
                type_,
                format,
            )?,
            TimeUnit::Millisecond => encoder.encode_field_with_type_and_format(
                &get_time32_millisecond_value(arr, idx),
                type_,
                format,
            )?,
            _ => {}
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => encoder.encode_field_with_type_and_format(
                &get_time64_microsecond_value(arr, idx),
                type_,
                format,
            )?,
            TimeUnit::Nanosecond => encoder.encode_field_with_type_and_format(
                &get_time64_nanosecond_value(arr, idx),
                type_,
                format,
            )?,
            _ => {}
        },
        DataType::Timestamp(unit, timezone) => match unit {
            TimeUnit::Second => {
                if arr.is_null(idx) {
                    return encoder.encode_field_with_type_and_format(
                        &None::<NaiveDateTime>,
                        type_,
                        format,
                    );
                }
                let ts_array = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                }
            }
            TimeUnit::Millisecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field_with_type_and_format(
                        &None::<NaiveDateTime>,
                        type_,
                        format,
                    );
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                }
            }
            TimeUnit::Microsecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field_with_type_and_format(
                        &None::<NaiveDateTime>,
                        type_,
                        format,
                    );
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                }
            }
            TimeUnit::Nanosecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field_with_type_and_format(
                        &None::<NaiveDateTime>,
                        type_,
                        format,
                    );
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field_with_type_and_format(&value, type_, format)?;
                }
            }
        },
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
            if arr.is_null(idx) {
                return encoder.encode_field_with_type_and_format(&None::<&[i8]>, type_, format);
            }
            let array = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            let value = encode_list(array, type_, format)?;
            encoder.encode_field_with_type_and_format(&value, type_, format)?
        }
        DataType::Struct(_) => {
            let fields = match type_.kind() {
                postgres_types::Kind::Composite(fields) => fields,
                _ => {
                    return Err(PgWireError::ApiError(ToSqlError::from(format!(
                        "Failed to unwrap a composite type from type {}",
                        type_
                    ))));
                }
            };
            let value = encode_struct(arr, idx, fields, format)?;
            encoder.encode_field_with_type_and_format(&value, type_, format)?
        }
        DataType::Dictionary(_, value_type) => {
            if arr.is_null(idx) {
                return encoder.encode_field_with_type_and_format(&None::<i8>, type_, format);
            }
            // Get the dictionary values, ignoring keys
            // We'll use Int32Type as a common key type, but we're only interested in values
            macro_rules! get_dict_values {
                ($key_type:ty) => {
                    arr.as_any()
                        .downcast_ref::<DictionaryArray<$key_type>>()
                        .map(|dict| dict.values())
                };
            }

            // Try to extract values using different key types
            let values = get_dict_values!(Int8Type)
                .or_else(|| get_dict_values!(Int16Type))
                .or_else(|| get_dict_values!(Int32Type))
                .or_else(|| get_dict_values!(Int64Type))
                .or_else(|| get_dict_values!(UInt8Type))
                .or_else(|| get_dict_values!(UInt16Type))
                .or_else(|| get_dict_values!(UInt32Type))
                .or_else(|| get_dict_values!(UInt64Type))
                .ok_or_else(|| {
                    ToSqlError::from(format!(
                        "Unsupported dictionary key type for value type {}",
                        value_type
                    ))
                })?;

            // If the dictionary has only one value, treat it as a primitive
            if values.len() == 1 {
                encode_value(encoder, values, 0, type_, format)?
            } else {
                // Otherwise, use value directly indexed by values array
                encode_value(encoder, values, idx, type_, format)?
            }
        }
        _ => {
            return Err(PgWireError::ApiError(ToSqlError::from(format!(
                "Unsupported Datatype {} and array {:?}",
                arr.data_type(),
                &arr
            ))));
        }
    }

    Ok(())
}
