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

mod get_objects;
use adbc_core::constants;
use datafusion::common::TableReference;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::vec::IntoIter;

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
    batches: IntoIter<RecordBatch>,
    schema: SchemaRef,
}

impl DataFusionReader {
    pub async fn new(df: DataFrame) -> std::result::Result<Self, DriverError> {
        let schema = df.schema().as_arrow().clone();

        Ok(Self {
            batches: df
                .collect()
                .await
                .map_err(ErrorHelper::from_datafusion)?
                .into_iter(),
            schema: schema.into(),
        })
    }
}

impl Iterator for DataFusionReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for DataFusionReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Default)]
pub struct DataFusionDriver {
    handle: Option<tokio::runtime::Handle>,
}

impl DataFusionDriver {
    pub fn new(handle: Option<tokio::runtime::Handle>) -> Self {
        Self { handle }
    }
}

impl Driver for DataFusionDriver {
    type DatabaseType = DataFusionDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType {
            handle: self.handle.clone(),
        })
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
        let mut database = Self::DatabaseType {
            handle: self.handle.clone(),
        };
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

pub struct DataFusionDatabase {
    handle: Option<tokio::runtime::Handle>,
}

impl Optionable for DataFusionDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        _value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        Err(ErrorHelper::set_unknown_option(&key).to_adbc())
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
        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);

        let runtime = Runtime::new(self.handle.clone()).map_err(|e| {
            ErrorHelper::io()
                .context("create Tokio runtime")
                .message(e.to_string())
                .to_adbc()
        })?;

        Ok(DataFusionConnection {
            runtime: Arc::new(runtime),
            ctx: Arc::new(ctx),
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
        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);

        let runtime = Runtime::new(self.handle.clone()).map_err(|e| {
            ErrorHelper::io()
                .context("create Tokio runtime")
                .message(e.to_string())
                .to_adbc()
        })?;

        let mut connection = DataFusionConnection {
            runtime: Arc::new(runtime),
            ctx: Arc::new(ctx),
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
            &format!("v{}", datafusion::arrow::ARROW_VERSION),
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
            bound_batches: None,
            bound_schema: None,
            ingest: BulkIngestState::new(),
        })
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        todo!()
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
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<arrow_schema::Schema> {
        todo!()
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        todo!()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        todo!()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        todo!()
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        todo!()
    }
}

enum QueryState {
    Sql(String),
    Substrait(Plan),
    Prepared(LogicalPlan),
}

pub struct DataFusionStatement {
    runtime: Arc<Runtime>,
    ctx: Arc<SessionContext>,
    query: Option<QueryState>,
    bound_batches: Option<Vec<RecordBatch>>,
    bound_schema: Option<SchemaRef>,
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
        let batches = self.bound_batches.take();
        let schema = self.bound_schema.take();

        let (batches, schema) = match (batches, schema) {
            (Some(b), Some(s)) => (b, s),
            _ => {
                return Err(ErrorHelper::invalid_state()
                    .message("no data bound for bulk ingest")
                    .to_adbc());
            }
        };

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
        let schema = batch.schema();
        self.bound_batches = Some(vec![batch]);
        self.bound_schema = Some(schema);
        Ok(())
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        let schema = reader.schema();
        let batches: std::result::Result<Vec<RecordBatch>, ArrowError> = reader.collect();
        let batches = batches.map_err(ErrorHelper::from_arrow)?;
        self.bound_batches = Some(batches);
        self.bound_schema = Some(schema);
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send>> {
        self.runtime.block_on(async {
            let df = match &self.query {
                Some(QueryState::Sql(query)) => self
                    .ctx
                    .sql(query)
                    .await
                    .map_err(ErrorHelper::from_datafusion)?,
                Some(QueryState::Substrait(plan)) => {
                    let plan = from_substrait_plan(&self.ctx.state(), plan)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?;
                    self.ctx
                        .execute_logical_plan(plan)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?
                }
                Some(QueryState::Prepared(plan)) => self
                    .ctx
                    .execute_logical_plan(plan.clone())
                    .await
                    .map_err(ErrorHelper::from_datafusion)?,
                None => {
                    return Err(ErrorHelper::invalid_state()
                        .message("no query or Substrait plan has been set")
                        .to_adbc());
                }
            };

            Ok(Box::new(DataFusionReader::new(df).await?) as Box<dyn RecordBatchReader + Send>)
        })
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        if self.ingest.is_set() {
            return self.execute_bulk_ingest();
        }

        self.runtime.block_on(async {
            let df = match &self.query {
                Some(QueryState::Sql(query)) => self
                    .ctx
                    .sql(query)
                    .await
                    .map_err(ErrorHelper::from_datafusion)?,
                Some(QueryState::Substrait(plan)) => {
                    let plan = from_substrait_plan(&self.ctx.state(), plan)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?;
                    self.ctx
                        .execute_logical_plan(plan)
                        .await
                        .map_err(ErrorHelper::from_datafusion)?
                }
                Some(QueryState::Prepared(plan)) => self
                    .ctx
                    .execute_logical_plan(plan.clone())
                    .await
                    .map_err(ErrorHelper::from_datafusion)?,
                None => {
                    return Err(ErrorHelper::invalid_state()
                        .message("no query or Substrait plan has been set")
                        .to_adbc());
                }
            };
            df.collect().await.map_err(ErrorHelper::from_datafusion)?;
            Ok::<_, adbc_core::error::Error>(())
        })?;

        Ok(Some(0))
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
        Err(ErrorHelper::not_implemented()
            .message("execute_partitions")
            .to_adbc())
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<arrow_schema::Schema> {
        Err(ErrorHelper::not_implemented()
            .message("get_parameter_schema")
            .to_adbc())
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

adbc_ffi::export_driver!(AdbcDriverDatafusionInit, DataFusionDriver);
