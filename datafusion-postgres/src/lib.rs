mod datatypes;
mod encoder;
mod handlers;
mod information_schema;

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use getset::{Getters, Setters, WithSetters};
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use handlers::HandlerFactory;
pub use handlers::{DfSessionService, Parser};

#[derive(Getters, Setters, WithSetters)]
#[getset(get = "pub", set = "pub", set_with = "pub")]
pub struct ServerOptions {
    host: String,
    port: u16,
}

impl ServerOptions {
    pub fn new() -> ServerOptions {
        ServerOptions::default()
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            host: "127.0.0.1".to_string(),
            port: 5432,
        }
    }
}

/// Serve the Datafusion `SessionContext` with Postgres protocol.
pub async fn serve(
    session_context: SessionContext,
    opts: &ServerOptions,
) -> Result<(), std::io::Error> {
    // Create the handler factory with the session context and catalog name
    let factory = Arc::new(HandlerFactory(Arc::new(DfSessionService::new(
        session_context,
    ))));

    // Bind to the specified host and port
    let server_addr = format!("{}:{}", opts.host, opts.port);
    let listener = TcpListener::bind(&server_addr).await?;
    println!("Listening on {}", server_addr);

    // Accept incoming connections
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let factory_ref = factory.clone();
                println!("Accepted connection from {}", addr);

                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, None, factory_ref).await {
                        eprintln!("Error processing socket: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accept socket: {}", e);
            }
        }
    }
}
