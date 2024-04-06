use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::*;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tokio::sync::Mutex;

use crate::datatypes::encode_dataframe;

pub(crate) struct DfSessionService {
    session_context: Arc<Mutex<SessionContext>>,
}

impl DfSessionService {
    pub fn new() -> DfSessionService {
        DfSessionService {
            session_context: Arc::new(Mutex::new(SessionContext::new())),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if query.starts_with("LOAD") {
            let command = query.trim_end();
            let command = command.strip_suffix(';').unwrap_or(command);
            let args = command.split(' ').collect::<Vec<&str>>();
            let table_name = args[2];
            let json_path = args[1];
            let ctx = self.session_context.lock().await;
            ctx.register_json(table_name, json_path, NdJsonReadOptions::default())
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
        } else if query.to_uppercase().starts_with("SELECT") {
            let ctx = self.session_context.lock().await;
            let df = ctx
                .sql(query)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let resp = encode_dataframe(df).await?;
            Ok(vec![Response::Query(resp)])
        } else {
            Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "Datafusion is a readonly execution engine. To load data, call\nLOAD json_file_path table_name;".to_owned(),
            )))])
        }
    }
}
