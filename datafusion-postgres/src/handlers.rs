use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::ast::{Expr, Ident, ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use tokio::sync::RwLock;

use crate::datatypes::{self, into_pg_type};

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
    pub session_context: Arc<RwLock<SessionContext>>,
    pub parser: Arc<Parser>,
    custom_session_vars: Arc<RwLock<HashMap<String, String>>>,
}

impl DfSessionService {
    pub fn new(session_context: SessionContext) -> DfSessionService {
        let session_context = Arc::new(RwLock::new(session_context));
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
        });
        DfSessionService {
            session_context,
            parser,
            custom_session_vars: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn handle_set(&self, variable: &ObjectName, value: &[Expr]) -> PgWireResult<()> {
        let var_name = variable
            .0
            .first()
            .map(|ident| ident.to_string().to_lowercase())
            .unwrap_or_default();

        let value_str = match value.first() {
            Some(Expr::Value(v)) => match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                sqlparser::ast::Value::Number(n, _) => n.to_string(),
                _ => v.to_string(),
            },
            Some(expr) => expr.to_string(),
            None => {
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "22023".to_string(),
                        "SET requires a value".to_string(),
                    ),
                )));
            }
        };

        match var_name.as_str() {
            "timezone" => {
                let mut sc_guard = self.session_context.write().await;
                let mut config = sc_guard.state().config().options().clone();
                config.execution.time_zone = Some(value_str);
                let new_context = SessionContext::new_with_config(config.into());
                let old_catalog_names = sc_guard.catalog_names();
                for catalog_name in old_catalog_names {
                    if let Some(catalog) = sc_guard.catalog(&catalog_name) {
                        for schema_name in catalog.schema_names() {
                            if let Some(schema) = catalog.schema(&schema_name) {
                                for table_name in schema.table_names() {
                                    if let Ok(Some(table)) = schema.table(&table_name).await {
                                        new_context
                                            .register_table(&table_name, table)
                                            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                                    }
                                }
                            }
                        }
                    }
                }
                *sc_guard = new_context;
                Ok(())
            }
            "client_encoding"
            | "search_path"
            | "application_name"
            | "datestyle"
            | "client_min_messages"
            | "extra_float_digits"
            | "standard_conforming_strings"
            | "check_function_bodies"
            | "transaction_read_only"
            | "transaction_isolation" => {
                let mut vars = self.custom_session_vars.write().await;
                vars.insert(var_name, value_str);
                Ok(())
            }
            _ => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(),
                    format!("Unrecognized configuration parameter '{}'", var_name),
                ),
            ))),
        }
    }

    async fn handle_show<'a>(&self, variable: &[Ident]) -> PgWireResult<QueryResponse<'a>> {
        let var_name = variable
            .first()
            .map(|ident| ident.to_string().to_lowercase())
            .unwrap_or_default();

        let sc_guard = self.session_context.read().await;
        let config = sc_guard.state().config().options().clone();

        let value = match var_name.as_str() {
            "timezone" => config
                .execution
                .time_zone
                .clone()
                .unwrap_or_else(|| "UTC".to_string()),
            "client_encoding" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "UTF8".to_string()),
            "search_path" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "public".to_string()),
            "application_name" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "".to_string()),
            "datestyle" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "ISO, MDY".to_string()),
            "client_min_messages" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "notice".to_string()),
            "extra_float_digits" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "3".to_string()),
            "standard_conforming_strings" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "on".to_string()),
            "check_function_bodies" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "off".to_string()),
            "transaction_read_only" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "off".to_string()),
            "transaction_isolation" => self
                .custom_session_vars
                .read()
                .await
                .get(&var_name)
                .cloned()
                .unwrap_or_else(|| "read committed".to_string()),

            // *** New variables to keep psql happy ***
            "server_version" => "14.0".to_string(),
            "server_version_num" => "140000".to_string(),
            "server_encoding" => "UTF8".to_string(),
            "is_superuser" => "off".to_string(),
            "lc_messages" => "en_US.UTF-8".to_string(),
            "lc_monetary" => "en_US.UTF-8".to_string(),
            "lc_numeric" => "en_US.UTF-8".to_string(),
            "lc_time" => "en_US.UTF-8".to_string(),

            "all" => {
                let mut names = Vec::new();
                let mut values = Vec::new();

                if let Some(tz) = &config.execution.time_zone {
                    names.push("timezone".to_string());
                    values.push(tz.clone());
                }

                let custom_vars = self.custom_session_vars.read().await;
                for (name, value) in custom_vars.iter() {
                    names.push(name.clone());
                    values.push(value.clone());
                }

                let defaults = vec![
                    ("client_encoding", "UTF8"),
                    ("search_path", "public"),
                    ("application_name", ""),
                    ("datestyle", "ISO, MDY"),
                    ("client_min_messages", "notice"),
                    ("extra_float_digits", "3"),
                    ("standard_conforming_strings", "on"),
                    ("check_function_bodies", "off"),
                    ("transaction_read_only", "off"),
                    ("transaction_isolation", "read committed"),
                    ("server_version", "14.0"),
                    ("server_version_num", "140000"),
                    ("server_encoding", "UTF8"),
                    ("is_superuser", "off"),
                    ("lc_messages", "en_US.UTF-8"),
                    ("lc_monetary", "en_US.UTF-8"),
                    ("lc_numeric", "en_US.UTF-8"),
                    ("lc_time", "en_US.UTF-8"),
                ];

                for (k, v) in defaults {
                    if !names.contains(&k.to_string()) {
                        names.push(k.to_string());
                        values.push(v.to_string());
                    }
                }

                let schema = Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("setting", DataType::Utf8, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(names)),
                        Arc::new(StringArray::from(values)),
                    ],
                )
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                let df = sc_guard
                    .read_batch(batch)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                return datatypes::encode_dataframe(df, &Format::UnifiedText).await;
            }

            _ => {
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_string(),
                        "42704".to_string(),
                        format!("Unrecognized configuration parameter '{}'", var_name),
                    ),
                )));
            }
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            &var_name,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![value]))])
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let df = sc_guard
            .read_batch(batch)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        datatypes::encode_dataframe(df, &Format::UnifiedText).await
    }
}

pub struct Parser {
    pub session_context: Arc<RwLock<SessionContext>>,
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let sc_guard = self.session_context.read().await;
        let state = sc_guard.state();
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
        let dialect = GenericDialect {};
        let stmts = SqlParser::parse_sql(&dialect, query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let mut responses = Vec::with_capacity(stmts.len());

        for statement in stmts {
            let stmt_string = statement.to_string().trim().to_owned();
            if stmt_string.is_empty() {
                continue;
            }
            match statement {
                Statement::SetVariable {
                    variables, value, ..
                } => {
                    let var = match variables {
                        sqlparser::ast::OneOrManyWithParens::One(ref name) => name,
                        sqlparser::ast::OneOrManyWithParens::Many(ref names) => {
                            names.first().unwrap()
                        }
                    };
                    self.handle_set(var, &value).await?;
                    responses.push(Response::Execution(pgwire::api::results::Tag::new("SET")));
                }
                Statement::ShowVariable { variable } => {
                    let resp = self.handle_show(&variable).await?;
                    responses.push(Response::Query(resp));
                }
                _ => {
                    let sc_guard = self.session_context.read().await;
                    let df = sc_guard
                        .sql(&stmt_string)
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    let resp = datatypes::encode_dataframe(df, &Format::UnifiedText).await?;
                    responses.push(Response::Query(resp));
                }
            }
        }
        Ok(responses)
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
        let stmt_string = portal.statement.id.clone();
        let stmt_upper = stmt_string.to_uppercase();

        // If the statement is a SET or SHOW, handle it here
        if stmt_upper.starts_with("SET ") {
            let dialect = GenericDialect {};
            let stmts = SqlParser::parse_sql(&dialect, &stmt_string)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            if let Statement::SetVariable {
                variables, value, ..
            } = &stmts[0]
            {
                let var = match variables {
                    sqlparser::ast::OneOrManyWithParens::One(ref name) => name,
                    sqlparser::ast::OneOrManyWithParens::Many(ref names) => names.first().unwrap(),
                };
                self.handle_set(var, value).await?;
                return Ok(Response::Execution(pgwire::api::results::Tag::new("SET")));
            }
        } else if stmt_upper.starts_with("SHOW ") {
            let dialect = GenericDialect {};
            let stmts = SqlParser::parse_sql(&dialect, &stmt_string)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            if let Statement::ShowVariable { variable } = &stmts[0] {
                let resp = self.handle_show(variable).await?;
                return Ok(Response::Query(resp));
            }
        }

        // Otherwise, treat it as a normal prepared statement
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

        let sc_guard = self.session_context.read().await;
        let dataframe = sc_guard
            .execute_logical_plan(plan)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let resp = datatypes::encode_dataframe(dataframe, &portal.result_column_format).await?;
        Ok(Response::Query(resp))
    }
}

fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    // Datafusion stores the parameters as a map.  In our case, the keys will be
    // `$1`, `$2` etc.  The values will be the parameter types.
    let mut types_vec = types.iter().collect::<Vec<_>>();
    types_vec.sort_by(|a, b| a.0.cmp(b.0));
    types_vec.into_iter().map(|pt| pt.1.as_ref()).collect()
}
