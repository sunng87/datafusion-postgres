// src/handlers.rs
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};

// --- Imports for multi-statement parsing ---
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
// ------------------------------------------------------

use crate::datatypes::{self, into_pg_type};

/// A factory that creates our handlers for the PGWire server.
pub struct HandlerFactory(pub Arc<DfSessionService>);

impl NoopStartupHandler for DfSessionService {}

impl PgWireServerHandlers for HandlerFactory {
    type StartupHandler = DfSessionService;
    type SimpleQueryHandler = DfSessionService;
    type ExtendedQueryHandler = DfSessionService;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.0.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

/// Our primary session service, storing a DataFusion `SessionContext`.
pub struct DfSessionService {
    pub session_context: Arc<SessionContext>,
    pub parser: Arc<Parser>,
}

impl DfSessionService {
    pub fn new(session_context: SessionContext) -> DfSessionService {
        let session_context = Arc::new(session_context);
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
        });
        DfSessionService {
            session_context,
            parser,
        }
    }
}

/// A simple parser that builds a logical plan from SQL text, using DataFusion.
pub struct Parser {
    session_context: Arc<SessionContext>,
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let context = &self.session_context;
        let state = context.state();

        let logical_plan = state
            .create_logical_plan(sql)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let optimized = state
            .optimize(&logical_plan)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        Ok(optimized)
    }
}

// ----------------------------------------------------------------
//   SimpleQueryHandler Implementation (multi-statement support)
// ----------------------------------------------------------------
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
        // 1) Parse the incoming query string into multiple statements using sqlparser.
        let dialect = GenericDialect {};
        let stmts = SqlParser::parse_sql(&dialect, query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // 2) For each parsed statement, execute with DataFusion and collect results.
        let mut responses = Vec::with_capacity(stmts.len());
        for statement in stmts {
            // Convert the AST statement back to SQL text.
            let stmt_string = statement.to_string().trim().to_owned();
            if stmt_string.is_empty() {
                continue;
            }

            // Intercept configuration commands (e.g. SET, SHOW) that are unsupported.
            let stmt_upper = stmt_string.to_uppercase();
            if stmt_upper.starts_with("SET ") || stmt_upper.starts_with("SHOW ") {
                // Return an empty query response for these commands.
                responses.push(Response::EmptyQuery);
                continue;
            }

            // Execute the statement in DataFusion.
            let df = self
                .session_context
                .sql(&stmt_string)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            // 3) Encode the DataFrame into a QueryResponse for the client.
            let resp = datatypes::encode_dataframe(df, &Format::UnifiedText).await?;
            responses.push(Response::Query(resp));
        }

        Ok(responses)
    }
}

// ----------------------------------------------------------------
//   ExtendedQueryHandler Implementation (same as original)
// ----------------------------------------------------------------
#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = LogicalPlan;
    type QueryParser = Parser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
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
        let fields = datatypes::df_schema_to_pg_fields(schema.as_ref(), &Format::UnifiedBinary)?;
        let params = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut param_types = Vec::with_capacity(params.len());
        for param_type in ordered_param_types(&params).iter() {
            if let Some(datatype) = param_type {
                let pgtype = into_pg_type(datatype)?;
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
        let format = &target.result_column_format;
        let schema = plan.schema();
        let fields = datatypes::df_schema_to_pg_fields(schema.as_ref(), format)?;

        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &portal.statement.statement;

        let param_types = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let param_values =
            datatypes::deserialize_parameters(portal, &ordered_param_types(&param_types))?;

        let plan = plan
            .clone()
            .replace_params_with_values(&param_values)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let dataframe = self
            .session_context
            .execute_logical_plan(plan)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let resp = datatypes::encode_dataframe(dataframe, &portal.result_column_format).await?;
        Ok(Response::Query(resp))
    }
}

/// Helper to convert DataFusion’s parameter map into an ordered list.
fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    // DataFusion stores parameters as a map keyed by "$1", "$2", etc.
    // We sort them in ascending order by key to match the expected parameter order.
    let mut types_vec = types.iter().collect::<Vec<_>>();
    types_vec.sort_by(|a, b| a.0.cmp(b.0));
    types_vec.into_iter().map(|pt| pt.1.as_ref()).collect()
}
