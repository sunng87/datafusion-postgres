use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use tokio::sync::Mutex;

use crate::datatypes;
use crate::information_schema::{columns_df, schemata_df, tables_df};
use pgwire::error::{PgWireError, PgWireResult};

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

pub struct DfSessionService {
    session_context: Arc<SessionContext>,
    parser: Arc<Parser>,
    timezone: Arc<Mutex<String>>,
    catalog_name: String,
}

impl DfSessionService {
    pub fn new(session_context: SessionContext, catalog_name: Option<String>) -> DfSessionService {
        let session_context = Arc::new(session_context);
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
        });
        let catalog_name = catalog_name.unwrap_or_else(|| {
            session_context
                .catalog_names()
                .first()
                .cloned()
                .unwrap_or_else(|| "datafusion".to_string())
        });
        DfSessionService {
            session_context,
            parser,
            timezone: Arc::new(Mutex::new("UTC".to_string())),
            catalog_name,
        }
    }

    fn mock_show_response<'a>(name: &str, value: &str) -> PgWireResult<QueryResponse<'a>> {
        let fields = vec![FieldInfo::new(
            name.to_string(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )];

        let row = {
            let mut encoder = pgwire::api::results::DataRowEncoder::new(Arc::new(fields.clone()));
            encoder.encode_field(&Some(value))?;
            encoder.finish()
        };

        let row_stream = futures::stream::once(async move { row });
        Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
    }

    // Mock pg_namespace response
    async fn mock_pg_namespace<'a>(&self) -> PgWireResult<QueryResponse<'a>> {
        let fields = vec![FieldInfo::new(
            "nspname".to_string(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )];

        let row = {
            let mut encoder = pgwire::api::results::DataRowEncoder::new(Arc::new(fields.clone()));
            encoder.encode_field(&Some(&self.catalog_name))?; // Return catalog_name as a schema
            encoder.finish()
        };

        let row_stream = futures::stream::once(async move { row });
        Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
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
        let query_lower = query.to_lowercase().trim().to_string();
        log::debug!("Received query: {}", query); // Log the query for debugging

        if query_lower.starts_with("set time zone") {
            let parts: Vec<&str> = query_lower.split_whitespace().collect();
            if parts.len() >= 4 {
                let tz = parts[3].trim_matches('"');
                let mut timezone = self.timezone.lock().await;
                *timezone = tz.to_string();
                return Ok(vec![Response::Execution(Tag::new("SET"))]);
            }
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "Invalid SET TIME ZONE syntax".to_string(),
                ),
            )));
        }

        if query_lower.starts_with("show ") {
            match query_lower.as_str() {
                "show time zone" => {
                    let timezone = self.timezone.lock().await.clone();
                    let resp = Self::mock_show_response("TimeZone", &timezone)?;
                    return Ok(vec![Response::Query(resp)]);
                }
                "show server_version" => {
                    let resp = Self::mock_show_response("server_version", "15.0 (DataFusion)")?;
                    return Ok(vec![Response::Query(resp)]);
                }
                "show transaction_isolation" => {
                    let resp =
                        Self::mock_show_response("transaction_isolation", "read uncommitted")?;
                    return Ok(vec![Response::Query(resp)]);
                }
                "show catalogs" => {
                    let catalogs = self.session_context.catalog_names();
                    let value = catalogs.join(", ");
                    let resp = Self::mock_show_response("Catalogs", &value)?;
                    return Ok(vec![Response::Query(resp)]);
                }
                "show search_path" => {
                    let resp = Self::mock_show_response("search_path", &self.catalog_name)?;
                    return Ok(vec![Response::Query(resp)]);
                }
                _ => {
                    return Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42704".to_string(),
                            format!("Unrecognized SHOW command: {}", query),
                        ),
                    )));
                }
            }
        }

        if query_lower.contains("information_schema.schemata") {
            let df = schemata_df(&self.session_context)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let resp = datatypes::encode_dataframe(df, &Format::UnifiedText).await?;
            return Ok(vec![Response::Query(resp)]);
        } else if query_lower.contains("information_schema.tables") {
            let df = tables_df(&self.session_context)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let resp = datatypes::encode_dataframe(df, &Format::UnifiedText).await?;
            return Ok(vec![Response::Query(resp)]);
        } else if query_lower.contains("information_schema.columns") {
            let df = columns_df(&self.session_context)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let resp = datatypes::encode_dataframe(df, &Format::UnifiedText).await?;
            return Ok(vec![Response::Query(resp)]);
        }

        // Handle pg_catalog.pg_namespace for pgcli compatibility
        if query_lower.contains("pg_catalog.pg_namespace") {
            let resp = self.mock_pg_namespace().await?;
            return Ok(vec![Response::Query(resp)]);
        }

        let ctx = &self.session_context;
        let qualified_query = if !query_lower.contains(&format!("{}.", self.catalog_name))
            && !query_lower.contains("information_schema")
            && !query_lower.contains("pg_catalog")
            && query_lower.contains("from")
        {
            query.replace(" FROM ", &format!(" FROM {}.", self.catalog_name))
        } else {
            query.to_string()
        };

        let df = ctx
            .sql(&qualified_query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let resp = datatypes::encode_dataframe(df, &Format::UnifiedText).await?;
        Ok(vec![Response::Query(resp)])
    }
}

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
            // Fixed: Use &params
            if let Some(datatype) = param_type {
                let pgtype = datatypes::into_pg_type(datatype)?;
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
        let query = portal
            .statement
            .statement
            .to_string()
            .to_lowercase()
            .trim()
            .to_string();
        log::debug!("Received extended query: {}", query); // Log for debugging

        if query.starts_with("show ") {
            match query.as_str() {
                "show time zone" => {
                    let timezone = self.timezone.lock().await.clone();
                    let resp = Self::mock_show_response("TimeZone", &timezone)?;
                    return Ok(Response::Query(resp));
                }
                "show server_version" => {
                    let resp = Self::mock_show_response("server_version", "15.0 (DataFusion)")?;
                    return Ok(Response::Query(resp));
                }
                "show transaction_isolation" => {
                    let resp =
                        Self::mock_show_response("transaction_isolation", "read uncommitted")?;
                    return Ok(Response::Query(resp));
                }
                "show catalogs" => {
                    let catalogs = self.session_context.catalog_names();
                    let value = catalogs.join(", ");
                    let resp = Self::mock_show_response("Catalogs", &value)?;
                    return Ok(Response::Query(resp));
                }
                "show search_path" => {
                    let resp = Self::mock_show_response("search_path", &self.catalog_name)?;
                    return Ok(Response::Query(resp));
                }
                _ => {
                    return Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "42704".to_string(),
                            format!("Unrecognized SHOW command: {}", query),
                        ),
                    )));
                }
            }
        }

        if query.contains("information_schema.schemata") {
            let df = schemata_df(&self.session_context)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let resp = datatypes::encode_dataframe(df, &portal.result_column_format).await?;
            return Ok(Response::Query(resp));
        } else if query.contains("information_schema.tables") {
            let df = tables_df(&self.session_context)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let resp = datatypes::encode_dataframe(df, &portal.result_column_format).await?;
            return Ok(Response::Query(resp));
        } else if query.contains("information_schema.columns") {
            let df = columns_df(&self.session_context)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let resp = datatypes::encode_dataframe(df, &portal.result_column_format).await?;
            return Ok(Response::Query(resp));
        }

        if query.contains("pg_catalog.pg_namespace") {
            let resp = self.mock_pg_namespace().await?;
            return Ok(Response::Query(resp));
        }

        let plan = &portal.statement.statement;
        let param_types = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let param_values =
            datatypes::deserialize_parameters(portal, &ordered_param_types(&param_types))?; // Fixed: Use &param_types
        let plan = plan
            .clone()
            .replace_params_with_values(&param_values)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?; // Fixed: Use &param_values
        let dataframe = self
            .session_context
            .execute_logical_plan(plan)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let resp = datatypes::encode_dataframe(dataframe, &portal.result_column_format).await?;
        Ok(Response::Query(resp))
    }
}

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
        let optimised = state
            .optimize(&logical_plan)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        Ok(optimised)
    }
}

fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    // Datafusion stores the parameters as a map.  In our case, the keys will be
    // `$1`, `$2` etc.  The values will be the parameter types.
    let mut types = types.iter().collect::<Vec<_>>();
    types.sort_by(|a, b| a.0.cmp(b.0));
    types.into_iter().map(|pt| pt.1.as_ref()).collect()
}
