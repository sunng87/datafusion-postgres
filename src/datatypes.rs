use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, ListArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::prelude::*;
use futures::{stream, StreamExt};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

pub(crate) fn into_pg_type(df_type: &DataType) -> PgWireResult<Type> {
    Ok(match df_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::UInt8 => Type::CHAR,
        DataType::Int16 | DataType::UInt16 => Type::INT2,
        DataType::Int32 | DataType::UInt32 => Type::INT4,
        DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Binary => Type::BYTEA,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Int8 | DataType::UInt8 => Type::CHAR_ARRAY,
            DataType::Int16 | DataType::UInt16 => Type::INT2_ARRAY,
            DataType::Int32 | DataType::UInt32 => Type::INT4_ARRAY,
            DataType::Int64 | DataType::UInt64 => Type::INT8_ARRAY,
            DataType::Timestamp(_, _) => Type::TIMESTAMP_ARRAY,
            DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
            DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
            DataType::Binary => Type::BYTEA_ARRAY,
            DataType::Float32 => Type::FLOAT4_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::Utf8 => Type::VARCHAR_ARRAY,
            list_type => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Unsupported List Datatype {list_type}"),
                ))));
            }
        },
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {df_type}"),
            ))));
        }
    })
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> bool {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(idx)
}

fn get_bool_list_value(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<bool>> {
    let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
    list_arr
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> $pt {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .value(idx)
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

macro_rules! get_primitive_list_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<$pt>> {
            let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            list_arr
                .as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .collect()
        }
    };

    ($name:ident, $t:ty, $pt:ty, $f:expr) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<$pt>> {
            let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            list_arr
                .as_any()
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

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> &str {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(idx)
}

fn get_utf8_list_value(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<String>> {
    let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
    list_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|opt| opt.map(|val| val.to_owned()))
        .collect()
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    arr: &Arc<dyn Array>,
    idx: usize,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx))?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx))?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx))?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx))?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx))?,
        DataType::UInt8 => encoder.encode_field(&(get_u8_value(arr, idx) as i8))?,
        DataType::UInt16 => encoder.encode_field(&(get_u16_value(arr, idx) as i16))?,
        DataType::UInt32 => encoder.encode_field(&get_u32_value(arr, idx))?,
        DataType::UInt64 => encoder.encode_field(&(get_u64_value(arr, idx) as i64))?,
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx))?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx))?,
        DataType::Utf8 => encoder.encode_field(&get_utf8_value(arr, idx))?,
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => encoder.encode_field(&get_bool_list_value(arr, idx))?,
            DataType::Int8 => encoder.encode_field(&get_i8_list_value(arr, idx))?,
            DataType::Int16 => encoder.encode_field(&get_i16_list_value(arr, idx))?,
            DataType::Int32 => encoder.encode_field(&get_i32_list_value(arr, idx))?,
            DataType::Int64 => encoder.encode_field(&get_i64_list_value(arr, idx))?,
            DataType::UInt8 => encoder.encode_field(&get_u8_list_value(arr, idx))?,
            DataType::UInt16 => encoder.encode_field(&get_u16_list_value(arr, idx))?,
            DataType::UInt32 => encoder.encode_field(&get_u32_list_value(arr, idx))?,
            DataType::UInt64 => encoder.encode_field(&get_u64_list_value(arr, idx))?,
            DataType::Float32 => encoder.encode_field(&get_f32_list_value(arr, idx))?,
            DataType::Float64 => encoder.encode_field(&get_f64_list_value(arr, idx))?,
            DataType::Utf8 => encoder.encode_field(&get_utf8_list_value(arr, idx))?,
            list_type => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!(
                        "Unsupported List Datatype {} and array {:?}",
                        list_type, &arr
                    ),
                ))))
            }
        },
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "Unsupported Datatype {} and array {:?}",
                    arr.data_type(),
                    &arr
                ),
            ))))
        }
    }
    Ok(())
}

//pub(crate) fn

pub(crate) fn df_schema_to_pg_fields(schema: &DFSchema) -> PgWireResult<Vec<FieldInfo>> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let pg_type = into_pg_type(f.data_type())?;
            Ok(FieldInfo::new(
                f.name().into(),
                None,
                None,
                pg_type,
                FieldFormat::Text,
            ))
        })
        .collect::<PgWireResult<Vec<FieldInfo>>>()
}

pub(crate) async fn encode_dataframe<'a>(df: DataFrame) -> PgWireResult<QueryResponse<'a>> {
    let fields = Arc::new(df_schema_to_pg_fields(df.schema())?);

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            let rb = rb.unwrap();
            let rows = rb.num_rows();
            let cols = rb.num_columns();

            let fields = fields_ref.clone();

            let row_stream = (0..rows).map(move |row| {
                let mut encoder = DataRowEncoder::new(fields.clone());
                for col in 0..cols {
                    let array = rb.column(col);
                    if array.is_null(row) {
                        encoder.encode_field(&None::<i8>).unwrap();
                    } else {
                        encode_value(&mut encoder, array, row).unwrap();
                    }
                }
                encoder.finish()
            });

            stream::iter(row_stream)
        })
        .flatten();

    Ok(QueryResponse::new(fields, pg_row_stream))
}
