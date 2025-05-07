use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    error::PgWireResult,
    messages::data::DataRow,
};

use super::encode_value;

pub struct RowEncoder {
    rb: RecordBatch,
    curr_idx: usize,
    fields: Arc<Vec<FieldInfo>>,
}

impl RowEncoder {
    pub fn new(rb: RecordBatch, fields: Arc<Vec<FieldInfo>>) -> Self {
        assert_eq!(rb.num_columns(), fields.len());
        Self {
            rb,
            fields,
            curr_idx: 0,
        }
    }

    pub fn next_row(&mut self) -> Option<PgWireResult<DataRow>> {
        if self.curr_idx == self.rb.num_rows() {
            return None;
        }
        let mut encoder = DataRowEncoder::new(self.fields.clone());
        for col in 0..self.rb.num_columns() {
            let array = self.rb.column(col);
            if array.is_null(self.curr_idx) {
                encoder.encode_field(&None::<i8>).unwrap();
            } else {
                let field = &self.fields[col];
                let type_ = field.datatype();
                let format = field.format();
                encode_value(&mut encoder, array, self.curr_idx, type_, format).unwrap();
            }
        }
        self.curr_idx += 1;
        Some(encoder.finish())
    }
}
