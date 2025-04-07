use std::sync::Arc;

use datafusion::execution::options::{
    ArrowReadOptions, AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions,
};
use datafusion::prelude::SessionContext;
use datafusion_postgres::{DfSessionService, HandlerFactory};
use pgwire::tokio::process_socket;
use structopt::StructOpt;
use tokio::net::TcpListener;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "datafusion-postgres",
    about = "A postgres interface for datatfusion. Serve any CSV/JSON/Arrow files as tables."
)]
struct Opt {
    /// CSV files to register as table, using syntax `table_name:file_path`
    #[structopt(long("csv"))]
    csv_tables: Vec<String>,
    /// JSON files to register as table, using syntax `table_name:file_path`
    #[structopt(long("json"))]
    json_tables: Vec<String>,
    /// Arrow files to register as table, using syntax `table_name:file_path`
    #[structopt(long("arrow"))]
    arrow_tables: Vec<String>,
    /// Parquet files to register as table, using syntax `table_name:file_path`
    #[structopt(long("parquet"))]
    parquet_tables: Vec<String>,
    /// Avro files to register as table, using syntax `table_name:file_path`
    #[structopt(long("avro"))]
    avro_tables: Vec<String>,
    /// Port the server listens to, default to 5432
    #[structopt(short, default_value = "5432")]
    port: u16,
    /// Host address the server listens to, default to 127.0.0.1
    #[structopt(long("host"), default_value = "127.0.0.1")]
    host: String,
}

fn parse_table_def(table_def: &str) -> (&str, &str) {
    table_def
        .split_once(':')
        .expect("Use this pattern to register table: table_name:file_path")
}

#[tokio::main]
async fn main() {
    let opts = Opt::from_args();

    let session_context = SessionContext::new();
    let mut registered_tables = Vec::new(); // Collect table names here

    for (table_name, table_path) in opts.csv_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_csv(table_name, table_path, CsvReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register table: {table_name}, {e}"));
        registered_tables.push(table_name.to_string());
        println!("Loaded {} as table {}", table_path, table_name);
    }

    for (table_name, table_path) in opts.json_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_json(table_name, table_path, NdJsonReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register table: {table_name}, {e}"));
        registered_tables.push(table_name.to_string());
        println!("Loaded {} as table {}", table_path, table_name);
    }

    for (table_name, table_path) in opts
        .arrow_tables
        .iter()
        .map(|s| parse_table_def(s.as_ref()))
    {
        session_context
            .register_arrow(table_name, table_path, ArrowReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register table: {table_name}, {e}"));
        registered_tables.push(table_name.to_string());
        println!("Loaded {} as table {}", table_path, table_name);
    }

    for (table_name, table_path) in opts
        .parquet_tables
        .iter()
        .map(|s| parse_table_def(s.as_ref()))
    {
        session_context
            .register_parquet(table_name, table_path, ParquetReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register table: {table_name}, {e}"));
        registered_tables.push(table_name.to_string());
        println!("Loaded {} as table {}", table_path, table_name);
    }

    for (table_name, table_path) in opts.avro_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_avro(table_name, table_path, AvroReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register table: {table_name}, {e}"));
        registered_tables.push(table_name.to_string());
        println!("Loaded {} as table {}", table_path, table_name);
    }

    let service = DfSessionService::new(session_context, registered_tables); // Pass registered_tables
    service
        .register_udfs()
        .await
        .unwrap_or_else(|e| panic!("Failed to register UDFs: {e}"));
    let factory = Arc::new(HandlerFactory(Arc::new(service)));

    let server_addr = format!("{}:{}", opts.host, opts.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
