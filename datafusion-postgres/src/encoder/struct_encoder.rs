use std::{error::Error, sync::Arc};

use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::{Array, StructArray};
use pgwire::{
    api::results::FieldFormat,
    error::PgWireResult,
    types::{ToSqlText, QUOTE_CHECK, QUOTE_ESCAPE},
};
use postgres_types::{Field, IsNull, ToSql, Type};

use super::{encode_value, EncodedValue};

pub fn encode_struct(
    arr: &Arc<dyn Array>,
    idx: usize,
    fields: &[Field],
    format: FieldFormat,
) -> Result<Option<EncodedValue>, Box<dyn Error + Send + Sync>> {
    let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    if arr.is_null(idx) {
        return Ok(None);
    }
    let mut row_encoder = StructEncoder::new(fields.len());
    for (i, arr) in arr.columns().iter().enumerate() {
        let field = &fields[i];
        let type_ = field.type_();
        encode_value(&mut row_encoder, arr, idx, type_, format).unwrap();
    }
    Ok(Some(EncodedValue {
        bytes: row_encoder.row_buffer,
    }))
}

struct StructEncoder {
    num_cols: usize,
    curr_col: usize,
    row_buffer: BytesMut,
}

impl StructEncoder {
    fn new(num_cols: usize) -> Self {
        Self {
            num_cols,
            curr_col: 0,
            row_buffer: BytesMut::new(),
        }
    }
}

impl super::Encoder for StructEncoder {
    fn encode_field_with_type_and_format<T>(
        &mut self,
        value: &T,
        data_type: &Type,
        format: FieldFormat,
    ) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        if format == FieldFormat::Text {
            if self.curr_col == 0 {
                self.row_buffer.put_slice(b"(");
            }
            // encode value in an intermediate buf
            let mut buf = BytesMut::new();
            value.to_sql_text(data_type, &mut buf)?;
            let encoded_value_as_str = String::from_utf8_lossy(&buf);
            if QUOTE_CHECK.is_match(&encoded_value_as_str) {
                self.row_buffer.put_u8(b'"');
                self.row_buffer.put_slice(
                    QUOTE_ESCAPE
                        .replace_all(&encoded_value_as_str, r#"\$1"#)
                        .as_bytes(),
                );
                self.row_buffer.put_u8(b'"');
            } else {
                self.row_buffer.put_slice(&buf);
            }
            if self.curr_col == self.num_cols - 1 {
                self.row_buffer.put_slice(b")");
            } else {
                self.row_buffer.put_slice(b",");
            }
        } else {
            if self.curr_col == 0 && format == FieldFormat::Binary {
                // Place Number of fields
                self.row_buffer.put_i32(self.num_cols as i32);
            }

            self.row_buffer.put_u32(data_type.oid());
            // remember the position of the 4-byte length field
            let prev_index = self.row_buffer.len();
            // write value length as -1 ahead of time
            self.row_buffer.put_i32(-1);
            let is_null = value.to_sql(data_type, &mut self.row_buffer)?;
            if let IsNull::No = is_null {
                let value_length = self.row_buffer.len() - prev_index - 4;
                let mut length_bytes = &mut self.row_buffer[prev_index..(prev_index + 4)];
                length_bytes.put_i32(value_length as i32);
            }
        }
        self.curr_col += 1;
        Ok(())
    }
}
