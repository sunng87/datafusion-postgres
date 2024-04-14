use std::sync::Arc;

use datafusion::execution::options::{CsvReadOptions, NdJsonReadOptions};
use datafusion::prelude::SessionContext;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::{MakeHandler, StatelessMakeHandler};
use pgwire::tokio::process_socket;
use structopt::StructOpt;
use tokio::net::TcpListener;

mod datatypes;
mod handlers;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "datafusion-postgres",
    about = "A postgres interface for datatfusion"
)]
struct Opt {
    #[structopt(short)]
    csv_tables: Vec<String>,
    #[structopt(short)]
    json_tables: Vec<String>,
}

fn parse_table_def(table_def: &String) -> (&str, &str) {
    table_def
        .split_once(':')
        .expect("Use this pattern to register table: table_name:file_path")
}

#[tokio::main]
async fn main() {
    let opts = Opt::from_args();

    let session_context = SessionContext::new();
    for (table_name, table_path) in opts.csv_tables.iter().map(parse_table_def) {
        session_context
            .register_csv(table_name, table_path, CsvReadOptions::default())
            .await
            .expect(&format!("Failed to register table: {table_name}"));
        println!("Loaded {} as table {}", table_path, table_name);
    }
    for (table_name, table_path) in opts.json_tables.iter().map(parse_table_def) {
        session_context
            .register_json(table_name, table_path, NdJsonReadOptions::default())
            .await
            .expect(&format!("Failed to register table: {table_name}"));
        println!("Loaded {} as table {}", table_path, table_name);
    }

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(
        handlers::DfSessionService::new(session_context),
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}
