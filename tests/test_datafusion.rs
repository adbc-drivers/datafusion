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

use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_datafusion::{ContextInit, DataFusionConnection, DataFusionDriver, DatabaseOpts};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use datafusion::prelude::*;
use std::error::Error as StdError;
use std::sync::Arc;

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use datafusion::datasource::MemTable;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;

fn get_connection(handle: Option<tokio::runtime::Handle>) -> DataFusionConnection {
    let mut driver = DataFusionDriver::new(handle);
    let database = driver.new_database().unwrap();
    database.new_connection().unwrap()
}

fn get_objects(connection: &DataFusionConnection) -> RecordBatch {
    let objects = connection.get_objects(
        adbc_core::options::ObjectDepth::All,
        None,
        None,
        None,
        None,
        None,
    );

    let batches: Vec<RecordBatch> = objects.unwrap().map(|b| b.unwrap()).collect();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

fn execute_update(connection: &mut DataFusionConnection, query: &str) {
    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query(query);
    let _ = statement.execute_update();
}

fn execute_sql_query(connection: &mut DataFusionConnection, query: &str) -> RecordBatch {
    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query(query);

    let batches: Vec<RecordBatch> = statement.execute().unwrap().map(|b| b.unwrap()).collect();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

fn try_execute_sql_query(
    connection: &mut DataFusionConnection,
    query: &str,
) -> Result<RecordBatch, Box<dyn StdError>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(query)?;

    let reader = statement.execute()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }

    let schema = match batches.first() {
        Some(batch) => batch.schema(),
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "query returned no batches",
            )
            .into());
        }
    };

    Ok(concat_batches(&schema, &batches)?)
}

fn answer_table(value: i32) -> datafusion::error::Result<MemTable> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "answer",
        DataType::Int32,
        false,
    )]));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![value]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
    MemTable::try_new(schema, vec![vec![batch]])
}

fn execute_substrait(connection: &mut DataFusionConnection, plan: Plan) -> RecordBatch {
    let mut statement = connection.new_statement().unwrap();

    let _ = statement.set_substrait_plan(plan.encode_to_vec());

    let batches: Vec<RecordBatch> = statement.execute().unwrap().map(|b| b.unwrap()).collect();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

#[test]
fn test_context_init_registers_table_provider() -> Result<(), Box<dyn StdError>> {
    let init: ContextInit = Arc::new(|ctx, _opts| {
        let table = answer_table(42)?;
        ctx.register_table("injected", Arc::new(table))?;
        Ok(())
    });
    let mut driver = DataFusionDriver::new_with_context_init(None, init);
    let database = driver.new_database()?;
    let mut connection = database.new_connection()?;

    let batch = try_execute_sql_query(&mut connection, "SELECT answer FROM injected")?;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    Ok(())
}

#[test]
fn test_context_init_receives_and_consumes_database_options() -> Result<(), Box<dyn StdError>> {
    let init: ContextInit = Arc::new(|ctx, opts: &mut DatabaseOpts| {
        let table_name = match opts.remove(&OptionDatabase::Other("custom.table_name".to_string()))
        {
            Some(OptionValue::String(table_name)) => table_name,
            Some(_) => {
                return Err(datafusion::error::DataFusionError::Configuration(
                    "custom.table_name must be a string".to_string(),
                ));
            }
            None => {
                return Err(datafusion::error::DataFusionError::Configuration(
                    "custom.table_name is required".to_string(),
                ));
            }
        };

        let table = answer_table(7)?;
        ctx.register_table(table_name, Arc::new(table))?;
        Ok(())
    });
    let mut driver = DataFusionDriver::new_with_context_init(None, init);
    let database = driver.new_database_with_opts(vec![
        (
            OptionDatabase::Uri,
            OptionValue::String("datafusion://".to_string()),
        ),
        (
            OptionDatabase::Other("custom.table_name".to_string()),
            OptionValue::String("custom_injected".to_string()),
        ),
    ])?;
    let mut connection = database.new_connection()?;

    let batch = try_execute_sql_query(&mut connection, "SELECT answer FROM custom_injected")?;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    Ok(())
}

#[test]
fn test_unconsumed_unknown_database_option_still_errors() -> Result<(), Box<dyn StdError>> {
    let mut driver = DataFusionDriver::new(None);
    let result = driver.new_database_with_opts(vec![(
        OptionDatabase::Other("custom.unconsumed".to_string()),
        OptionValue::String("value".to_string()),
    )]);

    let err = match result {
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "expected unconsumed database option to fail",
            )
            .into());
        }
        Err(err) => err,
    };

    assert_eq!(err.status, adbc_core::error::Status::NotImplemented);
    Ok(())
}

#[test]
fn test_context_init_failure_maps_to_adbc_error() -> Result<(), Box<dyn StdError>> {
    let init: ContextInit = Arc::new(|_ctx, _opts| {
        Err(datafusion::error::DataFusionError::Configuration(
            "hook failed".to_string(),
        ))
    });
    let mut driver = DataFusionDriver::new_with_context_init(None, init);
    let result = driver.new_database();

    let err = match result {
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "expected context init failure to fail database creation",
            )
            .into());
        }
        Err(err) => err,
    };

    assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
    assert!(err.message.contains("hook failed"));
    Ok(())
}

#[test]
fn test_connection_options() {
    let mut connection = get_connection(None);

    let current_catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();

    assert_eq!(current_catalog, "datafusion");

    // Create the secondary catalog and schema before switching
    let mut stmt = connection.new_statement().unwrap();
    stmt.set_sql_query("CREATE DATABASE IF NOT EXISTS datafusion2")
        .unwrap();
    stmt.execute_update().unwrap();

    connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("datafusion2".to_string()),
        )
        .unwrap();

    let current_catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();

    assert_eq!(current_catalog, "datafusion2");

    // Switch back and create a secondary schema
    connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("datafusion".to_string()),
        )
        .unwrap();

    let mut stmt = connection.new_statement().unwrap();
    stmt.set_sql_query("CREATE SCHEMA IF NOT EXISTS public2")
        .unwrap();
    stmt.execute_update().unwrap();

    let current_schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();

    assert_eq!(current_schema, "public");

    connection
        .set_option(
            OptionConnection::CurrentSchema,
            OptionValue::String("public2".to_string()),
        )
        .unwrap();

    let current_schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();

    assert_eq!(current_schema, "public2");

    // Verify setting nonexistent catalog/schema returns an error
    let err = connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("nonexistent".to_string()),
        )
        .unwrap_err();
    assert_eq!(err.status, adbc_core::error::Status::NotFound);

    let err = connection
        .set_option(
            OptionConnection::CurrentSchema,
            OptionValue::String("nonexistent".to_string()),
        )
        .unwrap_err();
    assert_eq!(err.status, adbc_core::error::Status::NotFound);
}

#[test]
fn test_get_objects_database() {
    let mut connection = get_connection(None);

    let objects = get_objects(&connection);

    assert_eq!(objects.num_rows(), 1);

    execute_update(&mut connection, "CREATE DATABASE another");

    let objects = get_objects(&connection);

    assert_eq!(objects.num_rows(), 2);
}

#[test]
fn test_execute_sql() {
    let mut connection = get_connection(None);

    execute_update(
        &mut connection,
        "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')",
    );

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn test_ingest() {
    let mut connection = get_connection(None);

    execute_update(
        &mut connection,
        "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')",
    );

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let mut statement = connection.new_statement().unwrap();

    let _ = statement.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("example".to_string()),
    );
    let _ = statement.set_option(
        OptionStatement::IngestMode,
        OptionValue::String(String::from(adbc_core::options::IngestMode::Append)),
    );
    let _ = statement.bind(batch);

    let _ = statement.execute_update();

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 6);
}

#[test]
fn test_execute_substrait() {
    let mut connection = get_connection(None);

    execute_update(
        &mut connection,
        "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')",
    );

    let ctx = SessionContext::new();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let plan = runtime.block_on(async {
        let _ = ctx.sql(
            "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')"
        ).await;

        let df = ctx.sql("SELECT c1, c2 FROM datafusion.public.example").await.unwrap();

        to_substrait_plan(df.logical_plan(), &ctx.state()).unwrap()
    });

    let batch = execute_substrait(&mut connection, *plan);

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_running_in_async() {
    let mut connection = get_connection(Some(tokio::runtime::Handle::current()));

    execute_update(
        &mut connection,
        "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')",
    );

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

/// A single-partition keyed table; a `GROUP BY k` over it hash-repartitions to
/// `target_partitions`, giving a plan whose output partition count is driven by config.
fn keyed_table(n_keys: i32) -> datafusion::error::Result<MemTable> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let keys: ArrayRef = Arc::new(Int32Array::from((0..n_keys).collect::<Vec<_>>()));
    let vals: ArrayRef = Arc::new(Int32Array::from(
        (0..n_keys).map(|i| i * 10).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![keys, vals])?;
    MemTable::try_new(schema, vec![vec![batch]])
}

/// A connection whose session pins `target_partitions` to `n` and has table `t` registered.
fn connection_with_target_partitions(n: usize) -> DataFusionConnection {
    let init: ContextInit = Arc::new(move |ctx, _opts| {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(n);
        *ctx = SessionContext::new_with_config(config);
        ctx.register_table("t", Arc::new(keyed_table(8)?))?;
        Ok(())
    });
    let mut driver = DataFusionDriver::new_with_context_init(None, init);
    let database = driver.new_database().unwrap();
    database.new_connection().unwrap()
}

const GROUP_BY_QUERY: &str = "SELECT k, sum(v) AS s FROM t GROUP BY k";

/// Collect the `k` column of every row produced by reading the given descriptor.
fn read_partition_keys(connection: &DataFusionConnection, descriptor: &[u8]) -> Vec<i32> {
    let reader = connection.read_partition(descriptor).unwrap();
    let mut keys = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..col.len() {
            keys.push(col.value(i));
        }
    }
    keys
}

#[test]
fn test_execute_partitions_returns_one_descriptor_per_output_partition() {
    let mut connection = connection_with_target_partitions(4);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(GROUP_BY_QUERY).unwrap();
    // GROUP_BY_QUERY shuffles, so the default `auto` mode would collapse it; force `multi`
    // to exercise one descriptor per natural partition.
    statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("multi".to_string()),
        )
        .unwrap();

    let result = statement.execute_partitions().unwrap();

    // Hash-repartitioned aggregate => one ADBC partition per target partition (4).
    assert_eq!(
        result.partitions.len(),
        4,
        "expected one descriptor per target partition, got {}",
        result.partitions.len()
    );
    assert_eq!(result.rows_affected, -1);

    // The union of all partitions reproduces every key exactly once.
    let mut keys = Vec::new();
    for descriptor in &result.partitions {
        keys.extend(read_partition_keys(&connection, descriptor));
    }
    keys.sort();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());
}

#[test]
fn test_read_partition_executes_serialized_plan() {
    // Plan with target_partitions = 4.
    let mut planner = connection_with_target_partitions(4);
    let mut statement = planner.new_statement().unwrap();
    statement.set_sql_query(GROUP_BY_QUERY).unwrap();
    statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("multi".to_string()),
        )
        .unwrap();
    let result = statement.execute_partitions().unwrap();
    assert!(result.partitions.len() > 1);

    // Execute on a connection whose default target_partitions differs (1). The descriptor
    // carries the already-built physical plan, so its partitioning is fixed regardless of the
    // executor's config — partition `i` stays meaningful and in range.
    let executor = connection_with_target_partitions(1);
    let mut keys = Vec::new();
    for descriptor in &result.partitions {
        keys.extend(read_partition_keys(&executor, descriptor));
    }
    keys.sort();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());
}

#[test]
fn test_execute_partitions_prepared() {
    // The proto path serializes the built physical plan regardless of source, so a prepared
    // statement partitions like any other query.
    let mut connection = connection_with_target_partitions(4);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(GROUP_BY_QUERY).unwrap();
    statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("multi".to_string()),
        )
        .unwrap();
    statement.prepare().unwrap();

    let result = statement.execute_partitions().unwrap();
    assert!(result.partitions.len() > 1);

    let mut keys = Vec::new();
    for descriptor in &result.partitions {
        keys.extend(read_partition_keys(&connection, descriptor));
    }
    keys.sort();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());
}

const PARTITION_MODE: &str = "datafusion.partition_mode";

/// A connection with a two-partition in-memory table `p` (keys 0..4 and 4..8) and no
/// repartitioning forced, so `SELECT * FROM p` is a shuffle-free two-partition scan.
fn connection_with_two_partition_table() -> DataFusionConnection {
    let init: ContextInit = Arc::new(|ctx, _opts| {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let part = |lo: i32, hi: i32| {
            let keys: ArrayRef = Arc::new(Int32Array::from((lo..hi).collect::<Vec<_>>()));
            let vals: ArrayRef = Arc::new(Int32Array::from(
                (lo..hi).map(|i| i * 10).collect::<Vec<_>>(),
            ));
            RecordBatch::try_new(schema.clone(), vec![keys, vals]).unwrap()
        };
        // Two batch groups => two table partitions.
        let table = MemTable::try_new(schema.clone(), vec![vec![part(0, 4)], vec![part(4, 8)]])?;
        ctx.register_table("p", Arc::new(table))?;
        Ok(())
    });
    let mut driver = DataFusionDriver::new_with_context_init(None, init);
    let database = driver.new_database().unwrap();
    database.new_connection().unwrap()
}

#[test]
fn test_partition_mode_single_collapses_to_one() {
    // GROUP_BY_QUERY hash-repartitions to 4 partitions; single mode must coalesce to one.
    let mut connection = connection_with_target_partitions(4);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(GROUP_BY_QUERY).unwrap();
    statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("single".to_string()),
        )
        .unwrap();

    let result = statement.execute_partitions().unwrap();
    assert_eq!(result.partitions.len(), 1, "single mode => one descriptor");

    // The lone descriptor reproduces every key — the coalesced plan runs the whole query.
    let mut keys = read_partition_keys(&connection, &result.partitions[0]);
    keys.sort();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());
}

#[test]
fn test_partition_mode_auto_collapses_shuffle() {
    // Aggregate has a shuffle => auto coalesces to one partition.
    let mut connection = connection_with_target_partitions(4);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(GROUP_BY_QUERY).unwrap();
    statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("auto".to_string()),
        )
        .unwrap();

    let result = statement.execute_partitions().unwrap();
    assert_eq!(
        result.partitions.len(),
        1,
        "auto mode collapses a shuffling plan to one partition"
    );
}

#[test]
fn test_partition_mode_auto_keeps_shuffle_free_partitions() {
    // A plain two-partition scan has no shuffle => auto keeps the natural partitions.
    let mut connection = connection_with_two_partition_table();
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT k, v FROM p").unwrap();
    statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("auto".to_string()),
        )
        .unwrap();

    let result = statement.execute_partitions().unwrap();
    assert_eq!(
        result.partitions.len(),
        2,
        "auto keeps natural partitions when the plan does not shuffle"
    );

    let mut keys = Vec::new();
    for descriptor in &result.partitions {
        keys.extend(read_partition_keys(&connection, descriptor));
    }
    keys.sort();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());
}

#[test]
fn test_partition_mode_rejects_unknown_value() {
    let mut connection = connection_with_target_partitions(4);
    let mut statement = connection.new_statement().unwrap();
    let err = statement
        .set_option(
            OptionStatement::Other(PARTITION_MODE.to_string()),
            OptionValue::String("nonsense".to_string()),
        )
        .unwrap_err();
    assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
}
