use std::ffi::OsStr;
use std::fs;

use datafusion::execution::options::{
    ArrowReadOptions, AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions,
};
use datafusion::prelude::SessionContext;
use datafusion_postgres::{serve, ServerOptions}; // Assuming the crate name is `datafusion_postgres`
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "datafusion-postgres",
    about = "A postgres interface for datafusion. Serve any CSV/JSON/Arrow files as tables."
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
    /// Directory to serve, all supported files will be registered as tables
    #[structopt(long("dir"), short("d"))]
    directory: Option<String>,
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

impl Opt {
    fn include_directory_files(&mut self) {
        if let Some(directory) = &self.directory {
            if let Ok(entries) = fs::read_dir(directory) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }

                    if let Some(ext) = path.extension().and_then(OsStr::to_str) {
                        let ext_lower = ext.to_lowercase();
                        if let Some(base_name) = path.file_stem().and_then(|s| s.to_str()) {
                            match ext_lower.as_ref() {
                                "json" => {
                                    self.json_tables.push(format!(
                                        "{}:{}",
                                        base_name,
                                        path.to_string_lossy()
                                    ));
                                }
                                "avro" => {
                                    self.avro_tables.push(format!(
                                        "{}:{}",
                                        base_name,
                                        path.to_string_lossy()
                                    ));
                                }
                                "parquet" => {
                                    self.parquet_tables.push(format!(
                                        "{}:{}",
                                        base_name,
                                        path.to_string_lossy()
                                    ));
                                }
                                "csv" => {
                                    self.csv_tables.push(format!(
                                        "{}:{}",
                                        base_name,
                                        path.to_string_lossy()
                                    ));
                                }
                                "arrow" => {
                                    self.arrow_tables.push(format!(
                                        "{}:{}",
                                        base_name,
                                        path.to_string_lossy()
                                    ));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn setup_session_context(
    session_context: &SessionContext,
    opts: &Opt,
) -> Result<(), Box<dyn std::error::Error>> {
    // Register CSV tables
    for (table_name, table_path) in opts.csv_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_csv(table_name, table_path, CsvReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register CSV table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register JSON tables
    for (table_name, table_path) in opts.json_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_json(table_name, table_path, NdJsonReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register JSON table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register Arrow tables
    for (table_name, table_path) in opts
        .arrow_tables
        .iter()
        .map(|s| parse_table_def(s.as_ref()))
    {
        session_context
            .register_arrow(table_name, table_path, ArrowReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register Arrow table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register Parquet tables
    for (table_name, table_path) in opts
        .parquet_tables
        .iter()
        .map(|s| parse_table_def(s.as_ref()))
    {
        session_context
            .register_parquet(table_name, table_path, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register Parquet table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register Avro tables
    for (table_name, table_path) in opts.avro_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_avro(table_name, table_path, AvroReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register Avro table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = Opt::from_args();
    opts.include_directory_files();

    let session_context = SessionContext::new();

    setup_session_context(&session_context, &opts).await?;

    let server_options = ServerOptions::new()
        .with_host(opts.host)
        .with_port(opts.port);

    serve(session_context, &server_options)
        .await
        .map_err(|e| format!("Failed to run server: {}", e))?;

    Ok(())
}
