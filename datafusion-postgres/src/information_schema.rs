use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, SessionContext};

/// Creates a DataFrame for the `information_schema.schemata` view.
pub async fn schemata_df(ctx: &SessionContext) -> Result<DataFrame, DataFusionError> {
    let catalog = ctx.catalog(ctx.catalog_names()[0].as_str()).unwrap(); // Use default catalog
    let schema_names: Vec<String> = catalog.schema_names();

    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("schema_owner", DataType::Utf8, true), // Nullable, not implemented
        Field::new("default_character_set_catalog", DataType::Utf8, true),
        Field::new("default_character_set_schema", DataType::Utf8, true),
        Field::new("default_character_set_name", DataType::Utf8, true),
    ]));

    let catalog_name = ctx.catalog_names()[0].clone(); // Use the first catalog name
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![catalog_name; schema_names.len()])),
            Arc::new(StringArray::from(schema_names.clone())), // Clone to avoid move
            Arc::new(StringArray::from(vec![None::<String>; schema_names.len()])),
            Arc::new(StringArray::from(vec![None::<String>; schema_names.len()])),
            Arc::new(StringArray::from(vec![None::<String>; schema_names.len()])),
            Arc::new(StringArray::from(vec![None::<String>; schema_names.len()])),
        ],
    )?;

    ctx.read_batch(record_batch) // Use read_batch instead of read_table
}

/// Creates a DataFrame for the `information_schema.tables` view.
pub async fn tables_df(ctx: &SessionContext) -> Result<DataFrame, DataFusionError> {
    let catalog = ctx.catalog(ctx.catalog_names()[0].as_str()).unwrap(); // Use default catalog
    let mut catalog_names = Vec::new();
    let mut schema_names = Vec::new();
    let mut table_names = Vec::new();
    let mut table_types = Vec::new();

    for schema_name in catalog.schema_names() {
        let schema = catalog.schema(&schema_name).unwrap();
        for table_name in schema.table_names() {
            catalog_names.push(ctx.catalog_names()[0].clone()); // Use the first catalog name
            schema_names.push(schema_name.clone());
            table_names.push(table_name.clone());
            table_types.push("BASE TABLE".to_string()); // DataFusion only has base tables
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(catalog_names)),
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(table_types)),
        ],
    )?;

    ctx.read_batch(record_batch) // Use read_batch instead of read_table
}

/// Creates a DataFrame for the `information_schema.columns` view.
pub async fn columns_df(ctx: &SessionContext) -> Result<DataFrame, DataFusionError> {
    let catalog = ctx.catalog(ctx.catalog_names()[0].as_str()).unwrap(); // Use default catalog
    let mut catalog_names = Vec::new();
    let mut schema_names = Vec::new();
    let mut table_names = Vec::new();
    let mut column_names = Vec::new();
    let mut ordinal_positions = Vec::new();
    let mut data_types = Vec::new();
    let mut is_nullables = Vec::new();

    for schema_name in catalog.schema_names() {
        let schema = catalog.schema(&schema_name).unwrap();
        for table_name in schema.table_names() {
            let table = schema
                .table(&table_name)
                .await
                .unwrap_or_else(|_| panic!("Table {} not found", table_name))
                .unwrap(); // Unwrap the Option after handling the Result
            let schema_ref = table.schema(); // Store SchemaRef in a variable
            let fields = schema_ref.fields(); // Borrow fields from the stored SchemaRef
            for (idx, field) in fields.iter().enumerate() {
                catalog_names.push(ctx.catalog_names()[0].clone()); // Use the first catalog name
                schema_names.push(schema_name.clone());
                table_names.push(table_name.clone());
                column_names.push(field.name().clone());
                ordinal_positions.push((idx + 1) as u32); // 1-based index
                data_types.push(field.data_type().to_string());
                is_nullables.push(field.is_nullable());
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::UInt32, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(catalog_names)),
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(UInt32Array::from(ordinal_positions)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(BooleanArray::from(is_nullables)),
        ],
    )?;

    ctx.read_batch(record_batch) // Use read_batch instead of read_table
}
