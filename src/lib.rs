// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod bind;
mod catalog;
mod get_objects;
mod object_storage;
use adbc_core::constants;
use datafusion::common::TableReference;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

use adbc_core::{
    Connection, Database, Driver, Optionable, Statement,
    error::Result,
    options::{
        InfoCode, IngestMode, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    },
};

use driverbase::bulk_ingest::BulkIngestState;
use driverbase::error::ErrorHelper as _;

#[derive(Clone, Copy, Debug)]
pub struct ErrorHelper {}

impl driverbase::error::ErrorHelper for ErrorHelper {
    const NAME: &'static str = "datafusion";
}

type DriverError = driverbase::error::Error<ErrorHelper>;

/// Database option values supplied during database initialization.
pub type DatabaseOpts = HashMap<OptionDatabase, OptionValue>;

/// Hook invoked after the `SessionContext` is constructed.
///
/// Downstream drivers can use this to register custom catalogs, schemas,
/// functions, or table providers. The hook may remove custom database options
/// that it consumes; any options left behind are handled by the base driver.
pub type ContextInit = Arc<
    dyn Fn(&mut SessionContext, &mut DatabaseOpts) -> datafusion::error::Result<()> + Send + Sync,
>;

impl ErrorHelper {
    fn from_datafusion(err: datafusion::error::DataFusionError) -> DriverError {
        match err {
            datafusion::error::DataFusionError::ArrowError(arrow_error, _) => {
                Self::from_arrow(*arrow_error)
            }
            datafusion::error::DataFusionError::AvroError(error) => {
                Self::io().message(error.to_string())
            }
            datafusion::error::DataFusionError::ParquetError(parquet_error) => {
                Self::io().message(parquet_error.to_string())
            }
            datafusion::error::DataFusionError::ObjectStore(error) => {
                Self::io().message(error.to_string())
            }
            datafusion::error::DataFusionError::IoError(error) => {
                Self::io().message(error.to_string())
            }
            datafusion::error::DataFusionError::SQL(parser_error, _) => {
                ErrorHelper::invalid_argument().message(parser_error.to_string())
            }
            datafusion::error::DataFusionError::NotImplemented(message) => {
                Self::not_implemented().message(message)
            }
            datafusion::error::DataFusionError::Internal(message) => {
                Self::internal_no_location().message(message)
            }
            datafusion::error::DataFusionError::Plan(message) => {
                Self::invalid_argument().message(message)
            }
            datafusion::error::DataFusionError::Configuration(message) => {
                Self::invalid_argument().message(message)
            }
            datafusion::error::DataFusionError::SchemaError(schema_error, _) => {
                Self::invalid_argument().message(schema_error.to_string())
            }
            datafusion::error::DataFusionError::Execution(message) => {
                Self::invalid_argument().message(message)
            }
            datafusion::error::DataFusionError::ExecutionJoin(join_error) => {
                Self::internal_no_location().message(join_error.to_string())
            }
            datafusion::error::DataFusionError::ResourcesExhausted(message) => {
                Self::internal_no_location().message(message)
            }
            datafusion::error::DataFusionError::External(error) => {
                Self::unknown().message(error.to_string())
            }
            datafusion::error::DataFusionError::Context(context, data_fusion_error) => {
                Self::from_datafusion(*data_fusion_error).context(context)
            }
            datafusion::error::DataFusionError::Substrait(message) => {
                Self::internal_no_location().message(message)
            }
            datafusion::error::DataFusionError::Diagnostic(_diagnostic, data_fusion_error) => {
                // TODO: process diagnostic (we need the source query though)
                Self::from_datafusion(*data_fusion_error)
            }
            datafusion::error::DataFusionError::Collection(data_fusion_errors) => {
                Self::from_all(data_fusion_errors.into_iter().map(Self::from_datafusion))
                    .unwrap_or(Self::unknown().message("unknown error"))
            }
            datafusion::error::DataFusionError::Shared(error) => {
                // Can't clone the error...
                Self::internal_no_location().message(error.to_string())
            }
            datafusion::error::DataFusionError::Ffi(message) => {
                ErrorHelper::internal_no_location().message(message)
            }
        }
    }
}

async fn register_object_store_for_plan(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> std::result::Result<(), datafusion::error::DataFusionError> {
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::logical_expr::DdlStatement;

    let location = match plan {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => &cmd.location,
        LogicalPlan::Copy(copy_to) => &copy_to.output_url,
        _ => return Ok(()),
    };

    let table_url = ListingTableUrl::parse(location)?;
    let scheme = table_url.scheme();
    let url = table_url.as_ref();

    if ctx
        .runtime_env()
        .object_store_registry
        .get_store(url)
        .is_err()
    {
        let state = ctx.state();
        let table_options = state.default_table_options();
        let store =
            object_storage::get_object_store(&state, scheme, url, &table_options, false).await?;
        ctx.runtime_env().register_object_store(url, store);
    }
    Ok(())
}

pub enum Runtime {
    Handle(tokio::runtime::Handle),
    Tokio(tokio::runtime::Runtime),
}

impl Runtime {
    pub fn new(handle: Option<tokio::runtime::Handle>) -> std::io::Result<Self> {
        if let Some(handle) = handle {
            Ok(Self::Handle(handle))
        } else {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            Ok(Self::Tokio(runtime))
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Runtime::Handle(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Runtime::Tokio(runtime) => runtime.block_on(future),
        }
    }
}

#[derive(Debug)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct DataFusionReader {
    runtime: Arc<Runtime>,
    stream: datafusion::execution::SendableRecordBatchStream,
    schema: SchemaRef,
}

impl DataFusionReader {
    pub async fn new(
        runtime: Arc<Runtime>,
        df: DataFrame,
    ) -> std::result::Result<Self, DriverError> {
        let schema = df.schema().as_arrow().clone();
        let stream = df
            .execute_stream()
            .await
            .map_err(ErrorHelper::from_datafusion)?;

        Ok(Self {
            runtime,
            stream,
            schema: schema.into(),
        })
    }

    /// Construct a reader directly from a single-partition stream, bypassing the
    /// `DataFrame` path. Used by `read_partition`, which executes one partition of an
    /// already-built physical plan.
    pub fn from_stream(
        runtime: Arc<Runtime>,
        stream: datafusion::execution::SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        Self {
            runtime,
            stream,
            schema,
        }
    }
}

impl Iterator for DataFusionReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_batch = self.runtime.block_on(async { self.stream.next().await });
        maybe_batch.map(|b| b.map_err(Into::into))
    }
}

impl RecordBatchReader for DataFusionReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct DataFusionDriver {
    handle: Option<tokio::runtime::Handle>,
    context_init: ContextInit,
}

impl DataFusionDriver {
    pub fn new(handle: Option<tokio::runtime::Handle>) -> Self {
        Self::new_with_context_init(handle, Arc::new(|_, _| Ok(())))
    }

    /// Create a driver that customizes each database's `SessionContext`.
    pub fn new_with_context_init(
        handle: Option<tokio::runtime::Handle>,
        context_init: ContextInit,
    ) -> Self {
        Self {
            handle,
            context_init,
        }
    }

    fn new_database_with_database_opts(
        &self,
        database_opts: &mut DatabaseOpts,
    ) -> Result<DataFusionDatabase> {
        let config = SessionConfig::new().with_information_schema(true);
        let mut ctx = SessionContext::new_with_config(config).enable_url_table();
        ctx.register_catalog_list(Arc::new(catalog::DynamicObjectStoreCatalog::new(
            ctx.state().catalog_list().clone(),
            ctx.state_weak_ref(),
        )));
        (self.context_init)(&mut ctx, database_opts).map_err(|error| {
            ErrorHelper::from_datafusion(error)
                .context("initialize DataFusion session context")
                .to_adbc()
        })?;
        Ok(DataFusionDatabase {
            handle: self.handle.clone(),
            ctx: Arc::new(ctx),
        })
    }
}

impl Default for DataFusionDriver {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Driver for DataFusionDriver {
    type DatabaseType = DataFusionDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        let mut database_opts = DatabaseOpts::default();
        self.new_database_with_database_opts(&mut database_opts)
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionDatabase,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut database_opts = opts.into_iter().collect::<DatabaseOpts>();
        let mut database = self.new_database_with_database_opts(&mut database_opts)?;
        for (key, value) in database_opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

pub struct DataFusionDatabase {
    handle: Option<tokio::runtime::Handle>,
    ctx: Arc<SessionContext>,
}

impl Optionable for DataFusionDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key {
            OptionDatabase::Uri => {
                // only support "datafusion://" for now
                let uri = ErrorHelper::option_as_string(&key, &value).map_err(|e| e.to_adbc())?;
                if uri == "datafusion://" {
                    Ok(())
                } else {
                    Err(ErrorHelper::set_invalid_option(&key, &value)
                        .message("only 'datafusion://' is accepted")
                        .to_adbc())
                }
            }
            _ => Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }
}

impl Database for DataFusionDatabase {
    type ConnectionType = DataFusionConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let runtime = Runtime::new(self.handle.clone()).map_err(|e| {
            ErrorHelper::io()
                .context("create Tokio runtime")
                .message(e.to_string())
                .to_adbc()
        })?;

        Ok(DataFusionConnection {
            runtime: Arc::new(runtime),
            ctx: self.ctx.clone(),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionConnection,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        let runtime = Runtime::new(self.handle.clone()).map_err(|e| {
            ErrorHelper::io()
                .context("create Tokio runtime")
                .message(e.to_string())
                .to_adbc()
        })?;

        let mut connection = DataFusionConnection {
            runtime: Arc::new(runtime),
            ctx: self.ctx.clone(),
        };

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}

pub struct DataFusionConnection {
    runtime: Arc<Runtime>,
    ctx: Arc<SessionContext>,
}

impl Optionable for DataFusionConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key.as_ref() {
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => match value {
                OptionValue::String(value) => {
                    if !self.ctx.catalog_names().contains(&value) {
                        return Err(ErrorHelper::not_found()
                            .context("set current catalog")
                            .format(format_args!("catalog '{value}' does not exist"))
                            .to_adbc());
                    }
                    self.runtime.block_on(async {
                        let query = format!("SET datafusion.catalog.default_catalog = {value}");
                        self.ctx
                            .sql(query.as_str())
                            .await
                            .map_err(ErrorHelper::from_datafusion)?
                            .collect()
                            .await
                            .map_err(ErrorHelper::from_datafusion)?;
                        Ok::<_, adbc_core::error::Error>(())
                    })?;
                    Ok(())
                }
                _ => Err(ErrorHelper::set_invalid_option(&key, &value)
                    .message("must be a string")
                    .to_adbc()),
            },
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => match value {
                OptionValue::String(value) => {
                    let state = self.ctx.state();
                    let catalog_name = &state.config_options().catalog.default_catalog;
                    let catalog = self.ctx.catalog(catalog_name).ok_or_else(|| {
                        ErrorHelper::not_found()
                            .context("set current schema")
                            .format(format_args!("catalog '{catalog_name}' does not exist"))
                            .to_adbc()
                    })?;
                    if !catalog.schema_names().contains(&value) {
                        return Err(ErrorHelper::not_found()
                            .context("set current schema")
                            .format(format_args!(
                                "schema '{value}' does not exist in catalog '{catalog_name}'"
                            ))
                            .to_adbc());
                    }
                    self.runtime.block_on(async {
                        let query = format!("SET datafusion.catalog.default_schema = {value}");
                        self.ctx
                            .sql(query.as_str())
                            .await
                            .map_err(ErrorHelper::from_datafusion)?
                            .collect()
                            .await
                            .map_err(ErrorHelper::from_datafusion)?;
                        Ok::<_, adbc_core::error::Error>(())
                    })?;
                    Ok(())
                }
                _ => Err(ErrorHelper::set_invalid_option(&key, &value)
                    .message("must be a string")
                    .to_adbc()),
            },
            _ => Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => Ok(self
                .ctx
                .state()
                .config_options()
                .catalog
                .default_catalog
                .clone()),
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => Ok(self
                .ctx
                .state()
                .config_options()
                .catalog
                .default_schema
                .clone()),
            _ => Err(ErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }
}

static INFO_CODES: std::sync::OnceLock<driverbase::InfoRegistry> = std::sync::OnceLock::new();

fn get_info_codes() -> &'static driverbase::InfoRegistry {
    INFO_CODES.get_or_init(|| {
        let mut registry = driverbase::InfoRegistry::new();
        registry.add_string(
            InfoCode::DriverName,
            "ADBC Driver Foundry Driver for Apache DataFusion",
        );
        registry.add_string(
            InfoCode::DriverVersion,
            concat!("v", env!("CARGO_PKG_VERSION")),
        );
        registry.add_string(InfoCode::VendorName, "Apache DataFusion");
        registry.add_string(InfoCode::VendorVersion, datafusion::DATAFUSION_VERSION);
        registry.add_string(
            InfoCode::DriverArrowVersion,
            format!("v{}", datafusion::arrow::ARROW_VERSION),
        );
        registry
    })
}

impl Connection for DataFusionConnection {
    type StatementType = DataFusionStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(DataFusionStatement {
            runtime: self.runtime.clone(),
            ctx: self.ctx.clone(),
            query: None,
            bound: None,
            ingest: BulkIngestState::new(),
        })
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        Err(ErrorHelper::not_implemented().message("cancel").to_adbc())
    }

    fn get_info(
        &self,
        codes: Option<std::collections::HashSet<adbc_core::options::InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let info = get_info_codes();
        Ok(Box::new(info.get_info(codes).build()))
    }

    fn get_objects(
        &self,
        depth: adbc_core::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let inner = get_objects::DataFusionGetObjects::new(self.ctx.clone(), self.runtime.clone());
        Ok(driverbase::get_objects::get_objects(
            inner,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> adbc_core::error::Result<arrow_schema::Schema> {
        let table_ref = match (catalog, db_schema) {
            (Some(catalog), Some(schema)) => TableReference::full(catalog, schema, table_name),
            (None, Some(schema)) => TableReference::partial(schema, table_name),
            _ => TableReference::bare(table_name),
        };

        self.runtime.block_on(async {
            let provider = self.ctx.table_provider(table_ref).await.map_err(|e| {
                ErrorHelper::not_found()
                    .context("get table schema")
                    .message(e.to_string())
            })?;
            Ok(provider.schema().as_ref().clone())
        })
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_table_types")
            .to_adbc())
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_statistic_names")
            .to_adbc())
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_statistics")
            .to_adbc())
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        Err(ErrorHelper::not_implemented().message("commit").to_adbc())
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        Err(ErrorHelper::not_implemented().message("rollback").to_adbc())
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let (target_partitions, index, query) = decode_descriptor(partition.as_ref())?;
        self.runtime.block_on(async {
            // Pin target_partitions so the physical plan matches the one execute_partitions
            // counted; DataFusion planning is deterministic only when this is held fixed.
            let state =
                datafusion::execution::session_state::SessionStateBuilder::new_from_existing(
                    self.ctx.state(),
                )
                .with_config(
                    self.ctx
                        .copied_config()
                        .with_target_partitions(target_partitions)
                        // The carried-over catalog list already holds the registered providers;
                        // recreating the default catalog here would wipe them.
                        .with_create_default_catalog_and_schema(false),
                )
                .build();

            let plan = match query {
                DescQuery::Substrait(bytes) => {
                    let proto = Plan::decode(bytes.as_slice()).map_err(|e| {
                        ErrorHelper::invalid_argument()
                            .message(e.to_string())
                            .to_adbc()
                    })?;
                    from_substrait_plan(&state, &proto)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?
                }
                DescQuery::Sql(sql) => state
                    .create_logical_plan(&sql)
                    .await
                    .map_err(ErrorHelper::from_datafusion)?,
            };
            register_object_store_for_plan(&self.ctx, &plan)
                .await
                .map_err(ErrorHelper::from_datafusion)?;
            let schema = plan.schema().as_arrow().clone();
            let physical = state
                .create_physical_plan(&plan)
                .await
                .map_err(ErrorHelper::from_datafusion)?;
            let stream = physical
                .execute(index as usize, state.task_ctx())
                .map_err(ErrorHelper::from_datafusion)?;
            Ok(Box::new(DataFusionReader::from_stream(
                self.runtime.clone(),
                stream,
                schema.into(),
            )) as Box<dyn RecordBatchReader + Send>)
        })
    }
}

enum QueryState {
    Sql(String),
    Substrait(Plan),
    Prepared(LogicalPlan),
}

impl QueryState {
    async fn execute(&self, ctx: &SessionContext) -> std::result::Result<DataFrame, DriverError> {
        let plan = match self {
            QueryState::Sql(query) => ctx
                .state()
                .create_logical_plan(query)
                .await
                .map_err(ErrorHelper::from_datafusion)?,
            QueryState::Substrait(plan) => from_substrait_plan(&ctx.state(), plan)
                .await
                .map_err(ErrorHelper::from_datafusion)?,
            QueryState::Prepared(plan) => plan.clone(),
        };
        register_object_store_for_plan(ctx, &plan)
            .await
            .map_err(ErrorHelper::from_datafusion)?;
        ctx.execute_logical_plan(plan)
            .await
            .map_err(ErrorHelper::from_datafusion)
    }
}

/// A query reconstructed from a partition descriptor. `Prepared` plans are not
/// representable here: they have no portable serialization without `datafusion-proto`.
enum DescQuery {
    Substrait(Vec<u8>),
    Sql(String),
}

/// Serialize a query for embedding in a partition descriptor. Returns the kind byte
/// (0 = Substrait, 1 = SQL) and the query bytes. `Prepared` statements are rejected
/// with `NOT_IMPLEMENTED` so clients fall back to single-partition execution.
fn encode_query(query: &QueryState) -> adbc_core::error::Result<(u8, Vec<u8>)> {
    match query {
        QueryState::Substrait(plan) => Ok((0, plan.encode_to_vec())),
        QueryState::Sql(sql) => Ok((1, sql.clone().into_bytes())),
        QueryState::Prepared(_) => Err(ErrorHelper::not_implemented()
            .message("partitioned execution of prepared statements")
            .to_adbc()),
    }
}

/// Build a self-contained partition descriptor:
/// `[u32 LE target_partitions][u32 LE index][u8 kind][query bytes...]`.
///
/// `target_partitions` is encoded so `read_partition` can pin it and re-plan into the
/// identical physical partitioning — see the determinism note on `read_partition`.
fn encode_descriptor(target_partitions: u32, index: u32, kind: u8, query: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + query.len());
    out.extend_from_slice(&target_partitions.to_le_bytes());
    out.extend_from_slice(&index.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(query);
    out
}

/// Inverse of [`encode_descriptor`]. Returns `(target_partitions, index, query)`.
fn decode_descriptor(bytes: &[u8]) -> adbc_core::error::Result<(usize, u32, DescQuery)> {
    if bytes.len() < 9 {
        return Err(ErrorHelper::invalid_argument()
            .message("short partition descriptor")
            .to_adbc());
    }
    let target_partitions = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let index = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let query = match bytes[8] {
        0 => DescQuery::Substrait(bytes[9..].to_vec()),
        1 => DescQuery::Sql(String::from_utf8(bytes[9..].to_vec()).map_err(|e| {
            ErrorHelper::invalid_argument()
                .message(e.to_string())
                .to_adbc()
        })?),
        other => {
            return Err(ErrorHelper::invalid_argument()
                .format(format_args!("unknown descriptor kind {other}"))
                .to_adbc());
        }
    };
    Ok((target_partitions, index, query))
}

pub struct DataFusionStatement {
    runtime: Arc<Runtime>,
    ctx: Arc<SessionContext>,
    query: Option<QueryState>,
    bound: Option<Box<dyn RecordBatchReader + Send>>,
    ingest: BulkIngestState<ErrorHelper>,
}

impl Optionable for DataFusionStatement {
    type Option = OptionStatement;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        if self
            .ingest
            .set_option(&key, &value)
            .map_err(|e| e.to_adbc())?
        {
            return Ok(());
        }
        match key.as_ref() {
            constants::ADBC_INGEST_OPTION_TEMPORARY => match value {
                OptionValue::String(v) if v == "false" => Ok(()),
                _ => Err(ErrorHelper::not_implemented()
                    .message("temporary tables are not supported")
                    .to_adbc()),
            },
            _ => Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
            constants::ADBC_INGEST_OPTION_TARGET_TABLE => match self.ingest.table {
                Some(ref table) => Ok(table.clone()),
                None => Err(ErrorHelper::not_found()
                    .format(format_args!("{key:?} has not been set"))
                    .to_adbc()),
            },
            _ => Err(ErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }
}

impl DataFusionStatement {
    fn ensure_prepared(&mut self) -> adbc_core::error::Result<LogicalPlan> {
        self.prepare()?;
        if let Some(QueryState::Prepared(plan)) = &self.query {
            Ok(plan.clone())
        } else {
            Err(ErrorHelper::invalid_state()
                .message("no query has been set")
                .to_adbc())
        }
    }

    fn execute_with_params(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let plan = self.ensure_prepared()?;
        Ok(Box::new(bind::BindReader::new(
            self.runtime.clone(),
            self.ctx.clone(),
            plan,
            reader,
        )))
    }

    fn execute_update_with_params(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<Option<i64>> {
        let plan = self.ensure_prepared()?;
        self.runtime.block_on(async {
            for batch in reader {
                let batch = batch.map_err(ErrorHelper::from_arrow)?;
                for row_idx in 0..batch.num_rows() {
                    let params = bind::row_to_scalar_values(&batch, row_idx)?;

                    let plan_with_params = plan
                        .clone()
                        .with_param_values(params)
                        .map_err(ErrorHelper::from_datafusion)?;
                    let df = self
                        .ctx
                        .execute_logical_plan(plan_with_params)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?;
                    df.collect().await.map_err(ErrorHelper::from_datafusion)?;
                }
            }
            Ok::<_, adbc_core::error::Error>(())
        })?;
        Ok(None)
    }

    fn make_table_ref(&self) -> TableReference {
        let table = self.ingest.table.as_deref().unwrap_or("");
        match (&self.ingest.catalog, &self.ingest.schema) {
            (Some(catalog), Some(schema)) => {
                TableReference::full(catalog.as_str(), schema.as_str(), table)
            }
            (None, Some(schema)) => TableReference::partial(schema.as_str(), table),
            _ => TableReference::bare(table),
        }
    }

    fn execute_bulk_ingest(&mut self) -> adbc_core::error::Result<Option<i64>> {
        let reader = self.bound.take().ok_or_else(|| {
            ErrorHelper::invalid_state()
                .message("no data bound for bulk ingest")
                .to_adbc()
        })?;

        let schema = reader.schema();
        let batches: std::result::Result<Vec<RecordBatch>, ArrowError> = reader.collect();
        let batches = batches.map_err(ErrorHelper::from_arrow)?;

        let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
        let table_ref = self.make_table_ref();

        self.runtime.block_on(async {
            match self.ingest.mode {
                IngestMode::Create => {
                    if self
                        .ctx
                        .table_exist(table_ref.clone())
                        .map_err(ErrorHelper::from_datafusion)?
                    {
                        return Err(ErrorHelper::already_exists()
                            .format(format_args!(
                                "table '{}' already exists",
                                self.ingest.table.as_deref().unwrap_or("")
                            ))
                            .to_adbc());
                    }
                    let mem_table = MemTable::try_new(schema, vec![batches])
                        .map_err(ErrorHelper::from_datafusion)?;
                    self.ctx
                        .register_table(table_ref, Arc::new(mem_table))
                        .map_err(ErrorHelper::from_datafusion)?;
                }
                IngestMode::Append => {
                    if !self
                        .ctx
                        .table_exist(table_ref.clone())
                        .map_err(ErrorHelper::from_datafusion)?
                    {
                        return Err(ErrorHelper::not_found()
                            .format(format_args!(
                                "Not found: Table '{}'",
                                self.ingest.table.as_deref().unwrap_or("")
                            ))
                            .to_adbc());
                    }
                    let df = self
                        .ctx
                        .read_batches(batches)
                        .map_err(ErrorHelper::from_datafusion)?;
                    df.write_table(
                        &table_ref.to_string(),
                        DataFrameWriteOptions::new().with_insert_operation(InsertOp::Append),
                    )
                    .await
                    .map_err(ErrorHelper::from_datafusion)?;
                }
                IngestMode::Replace => {
                    if self
                        .ctx
                        .table_exist(table_ref.clone())
                        .map_err(ErrorHelper::from_datafusion)?
                    {
                        self.ctx
                            .deregister_table(table_ref.clone())
                            .map_err(ErrorHelper::from_datafusion)?;
                    }
                    let mem_table = MemTable::try_new(schema, vec![batches])
                        .map_err(ErrorHelper::from_datafusion)?;
                    self.ctx
                        .register_table(table_ref, Arc::new(mem_table))
                        .map_err(ErrorHelper::from_datafusion)?;
                }
                IngestMode::CreateAppend => {
                    if self
                        .ctx
                        .table_exist(table_ref.clone())
                        .map_err(ErrorHelper::from_datafusion)?
                    {
                        let df = self
                            .ctx
                            .read_batches(batches)
                            .map_err(ErrorHelper::from_datafusion)?;
                        df.write_table(
                            &table_ref.to_string(),
                            DataFrameWriteOptions::new().with_insert_operation(InsertOp::Append),
                        )
                        .await
                        .map_err(ErrorHelper::from_datafusion)?;
                    } else {
                        let mem_table = MemTable::try_new(schema, vec![batches])
                            .map_err(ErrorHelper::from_datafusion)?;
                        self.ctx
                            .register_table(table_ref, Arc::new(mem_table))
                            .map_err(ErrorHelper::from_datafusion)?;
                    }
                }
            }
            Ok(())
        })?;

        self.ingest.clear();
        Ok(Some(row_count))
    }
}

impl Statement for DataFusionStatement {
    fn bind(&mut self, batch: arrow_array::RecordBatch) -> adbc_core::error::Result<()> {
        self.bound = Some(Box::new(SingleBatchReader::new(batch)));
        Ok(())
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        self.bound = Some(reader);
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send>> {
        if let Some(reader) = self.bound.take() {
            return self.execute_with_params(reader);
        }

        self.runtime.block_on(async {
            let df = match &self.query {
                Some(q) => q.execute(&self.ctx).await?,
                None => {
                    return Err(ErrorHelper::invalid_state()
                        .message("no query or Substrait plan has been set")
                        .to_adbc());
                }
            };

            Ok(
                Box::new(DataFusionReader::new(self.runtime.clone(), df).await?)
                    as Box<dyn RecordBatchReader + Send>,
            )
        })
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        if self.ingest.is_set() {
            return self.execute_bulk_ingest();
        }

        if let Some(reader) = self.bound.take() {
            return self.execute_update_with_params(reader);
        }

        self.runtime.block_on(async {
            let df = match &self.query {
                Some(q) => q.execute(&self.ctx).await?,
                None => {
                    return Err(ErrorHelper::invalid_state()
                        .message("no query or Substrait plan has been set")
                        .to_adbc());
                }
            };
            df.collect().await.map_err(ErrorHelper::from_datafusion)?;
            Ok::<_, adbc_core::error::Error>(())
        })?;
        Ok(None)
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<arrow_schema::Schema> {
        self.runtime.block_on(async {
            match &self.query {
                Some(QueryState::Sql(query)) => {
                    let df = self
                        .ctx
                        .sql(query)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?;
                    Ok(df.schema().as_arrow().clone())
                }
                Some(QueryState::Substrait(plan)) => {
                    let plan = from_substrait_plan(&self.ctx.state(), plan)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?;
                    Ok(plan.schema().as_arrow().clone())
                }
                Some(QueryState::Prepared(plan)) => Ok(plan.schema().as_arrow().clone()),
                None => Err(ErrorHelper::invalid_state()
                    .message("no query has been set")
                    .to_adbc()),
            }
        })
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<adbc_core::PartitionedResult> {
        let query = self.query.as_ref().ok_or_else(|| {
            ErrorHelper::invalid_state()
                .message("no query or Substrait plan has been set")
                .to_adbc()
        })?;
        self.runtime.block_on(async {
            // Plan logically (registers object store) and build the physical plan so we
            // can count its natural output partitions.
            let df = query.execute(&self.ctx).await?;
            let schema = df.schema().as_arrow().clone();
            let physical = df
                .create_physical_plan()
                .await
                .map_err(ErrorHelper::from_datafusion)?;
            let n = physical.output_partitioning().partition_count() as u32;
            // Capture target_partitions so read_partition re-plans identically.
            let target_partitions = self.ctx.copied_config().target_partitions() as u32;
            let (kind, query_bytes) = encode_query(query)?;
            let partitions = (0..n)
                .map(|i| encode_descriptor(target_partitions, i, kind, &query_bytes))
                .collect::<Vec<_>>();
            Ok(adbc_core::PartitionedResult {
                partitions,
                schema,
                rows_affected: -1,
            })
        })
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<arrow_schema::Schema> {
        let param_types = match &self.query {
            Some(QueryState::Prepared(plan)) => plan
                .get_parameter_types()
                .map_err(ErrorHelper::from_datafusion)
                .map_err(|e| e.to_adbc()),
            Some(QueryState::Sql(sql)) => {
                let plan = self
                    .runtime
                    .block_on(async {
                        self.ctx
                            .state()
                            .create_logical_plan(sql)
                            .await
                            .map_err(ErrorHelper::from_datafusion)
                    })
                    .map_err(|e| e.to_adbc())?;
                plan.get_parameter_types()
                    .map_err(ErrorHelper::from_datafusion)
                    .map_err(|e| e.to_adbc())
            }
            Some(QueryState::Substrait(plan)) => {
                let logical_plan = self
                    .runtime
                    .block_on(async {
                        from_substrait_plan(&self.ctx.state(), plan)
                            .await
                            .map_err(ErrorHelper::from_datafusion)
                    })
                    .map_err(|e| e.to_adbc())?;
                logical_plan
                    .get_parameter_types()
                    .map_err(ErrorHelper::from_datafusion)
                    .map_err(|e| e.to_adbc())
            }
            _ => {
                return Err(ErrorHelper::invalid_state()
                    .message("no query has been set")
                    .to_adbc());
            }
        }?;

        let mut params: Vec<_> = param_types.into_iter().collect();
        params.sort_by_key(|(name, _)| name.trim_start_matches('$').parse::<usize>().unwrap_or(0));

        let fields: Vec<arrow_schema::Field> = params
            .into_iter()
            .map(|(name, dt)| {
                let data_type = dt.unwrap_or(arrow_schema::DataType::Null);
                arrow_schema::Field::new(name, data_type, true)
            })
            .collect();

        Ok(arrow_schema::Schema::new(fields))
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        match self.query.take() {
            Some(QueryState::Sql(sql)) => {
                let plan = self.runtime.block_on(async {
                    self.ctx
                        .state()
                        .create_logical_plan(&sql)
                        .await
                        .map_err(ErrorHelper::from_datafusion)
                })?;
                self.query = Some(QueryState::Prepared(plan));
            }
            Some(QueryState::Substrait(plan)) => {
                let logical_plan = self.runtime.block_on(async {
                    from_substrait_plan(&self.ctx.state(), &plan)
                        .await
                        .map_err(ErrorHelper::from_datafusion)
                })?;
                self.query = Some(QueryState::Prepared(logical_plan));
            }
            Some(prepared @ QueryState::Prepared(_)) => {
                self.query = Some(prepared);
            }
            None => {
                return Err(ErrorHelper::invalid_state()
                    .message("no query has been set")
                    .to_adbc());
            }
        }
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
        self.query = Some(QueryState::Sql(query.as_ref().to_string()));
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        self.query = Some(QueryState::Substrait(Plan::decode(plan.as_ref()).map_err(
            |e| {
                ErrorHelper::invalid_argument()
                    .context("decode Substrait plan")
                    .message(e.to_string())
                    .to_adbc()
            },
        )?));
        Ok(())
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        Err(ErrorHelper::not_implemented().message("cancel").to_adbc())
    }
}

#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(AdbcDriverDatafusionInit, DataFusionDriver);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_round_trip_sql() {
        let bytes = encode_descriptor(8, 3, 1, b"SELECT 1");
        let (target_partitions, index, query) = decode_descriptor(&bytes).unwrap();
        assert_eq!(target_partitions, 8);
        assert_eq!(index, 3);
        match query {
            DescQuery::Sql(sql) => assert_eq!(sql, "SELECT 1"),
            _ => panic!("expected SQL"),
        }
    }

    #[test]
    fn descriptor_round_trip_substrait() {
        let plan_bytes = vec![1u8, 2, 3, 4, 5];
        let bytes = encode_descriptor(4, 0, 0, &plan_bytes);
        let (target_partitions, index, query) = decode_descriptor(&bytes).unwrap();
        assert_eq!(target_partitions, 4);
        assert_eq!(index, 0);
        match query {
            DescQuery::Substrait(b) => assert_eq!(b, plan_bytes),
            _ => panic!("expected Substrait"),
        }
    }

    #[test]
    fn decode_rejects_short_descriptor() {
        assert!(decode_descriptor(&[0u8; 8]).is_err());
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let bytes = encode_descriptor(1, 0, 9, b"x");
        assert!(decode_descriptor(&bytes).is_err());
    }

    #[test]
    fn encode_query_rejects_prepared() {
        let plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        assert!(encode_query(&QueryState::Prepared(plan)).is_err());
    }
}
