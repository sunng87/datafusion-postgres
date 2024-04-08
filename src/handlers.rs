use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::DFParser;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use tokio::sync::Mutex;

use crate::datatypes::{self, into_pg_type};

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

            let resp = datatypes::encode_dataframe(df).await?;
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

pub(crate) struct Parser {
    service: Arc<DfSessionService>,
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let context = self.service.session_context.lock().await;
        let state = context.state();

        let logical_plan = state
            .create_logical_plan(sql)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let optimised = state
            .optimize(&logical_plan)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        Ok(optimised)
    }
}

#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = LogicalPlan;

    type QueryParser = Parser;

    #[doc = " Get a reference to associated `QueryParser` implementation"]
    fn query_parser(&self) -> Arc<Self::QueryParser> {
        todo!()
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &target.statement;

        let schema = plan.schema();
        let fields = datatypes::df_schema_to_pg_fields(schema.as_ref())?;
        let params = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut param_types = Vec::with_capacity(params.len());
        for param_type in params.into_values() {
            if let Some(datatype) = param_type {
                let pgtype = into_pg_type(&datatype)?;
                param_types.push(pgtype);
            } else {
                param_types.push(Type::UNKNOWN);
            }
        }

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &target.statement.statement;
        let schema = plan.schema();
        let fields = datatypes::df_schema_to_pg_fields(schema.as_ref())?;

        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!()
    }
}
