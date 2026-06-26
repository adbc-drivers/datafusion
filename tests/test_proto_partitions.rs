// Copyright (c) 2025-2026 ADBC Drivers Contributors
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

//! Proto path tests: a custom `TableProvider` emits a custom `ExecutionPlan`, and a
//! `PhysicalExtensionCodec` round-trips it. `execute_partitions` serializes the physical
//! plan; a *fresh* connection's `read_partition` reproduces the rows by deserializing the
//! plan — the descriptor carries only the serialized plan, so re-planning is structurally
//! impossible and a passing test proves the deserialize path. Also covers the hard error
//! when a custom node cannot be encoded (no re-plan fallback).

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_datafusion::{ContextInit, DataFusionConnection, DataFusionDriver, PhysicalCodec};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

/// Single `Int32` column named `v`.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
}

/// A custom scan node: holds the full data and a partition count; partition `i` emits a
/// round-robin slice (`data[i], data[i + n], ...`). Carries everything needed to rebuild
/// itself from bytes, so it survives proto round-trip via [`CustomCodec`].
#[derive(Debug)]
struct CustomExec {
    data: Vec<i32>,
    partitions: usize,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl CustomExec {
    fn new(data: Vec<i32>, partitions: usize) -> Self {
        let schema = schema();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            data,
            partitions,
            schema,
            props,
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CustomExec(partitions={})", self.partitions)
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut vals = Vec::new();
        let mut i = partition;
        while i < self.data.len() {
            vals.push(self.data[i]);
            i += self.partitions;
        }
        let col: ArrayRef = Arc::new(Int32Array::from(vals));
        let batch = RecordBatch::try_new(self.schema.clone(), vec![col])?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema.clone(),
            None,
        )?))
    }
}

/// A table provider whose `scan` returns a [`CustomExec`].
#[derive(Debug)]
struct CustomProvider {
    data: Vec<i32>,
    partitions: usize,
}

#[async_trait]
impl TableProvider for CustomProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(
            self.data.clone(),
            self.partitions,
        )))
    }
}

/// Codec for [`CustomExec`]: encodes `partitions` then the data as little-endian i32s.
#[derive(Debug)]
struct CustomCodec;

impl PhysicalExtensionCodec for CustomCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if buf.len() < 8 {
            return Err(DataFusionError::Internal("short CustomExec buffer".into()));
        }
        let partitions = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
        let data: Vec<i32> = buf[8..]
            .chunks_exact(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        Ok(Arc::new(CustomExec::new(data, partitions)))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        let exec = node
            .as_any()
            .downcast_ref::<CustomExec>()
            .ok_or_else(|| DataFusionError::Internal("not a CustomExec".into()))?;
        buf.extend_from_slice(&(exec.partitions as u64).to_le_bytes());
        for v in &exec.data {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Ok(())
    }
}

/// A codec that refuses to encode, to exercise the hard error when a custom node cannot
/// be serialized (no re-plan fallback).
#[derive(Debug)]
struct FailingCodec;

impl PhysicalExtensionCodec for FailingCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("decode unsupported".into()))
    }

    fn try_encode(&self, _node: Arc<dyn ExecutionPlan>, _buf: &mut Vec<u8>) -> DFResult<()> {
        Err(DataFusionError::Internal("encode unsupported".into()))
    }
}

/// Delegates to [`CustomCodec`] but counts `try_decode` calls, to prove the per-connection
/// plan cache deserializes a given plan only once.
#[derive(Debug)]
struct CountingCodec {
    decodes: Arc<AtomicUsize>,
}

impl PhysicalExtensionCodec for CountingCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.decodes.fetch_add(1, Ordering::SeqCst);
        CustomCodec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        CustomCodec.try_encode(node, buf)
    }
}

const VALUES: i32 = 8;
const N_PARTITIONS: usize = 3;
const QUERY: &str = "SELECT v FROM ct";

/// Build a connection whose `ct` table is the custom provider, with the given codec (if any).
fn custom_connection(codec: Option<PhysicalCodec>) -> DataFusionConnection {
    let init: ContextInit = Arc::new(|ctx, _opts| {
        ctx.register_table(
            "ct",
            Arc::new(CustomProvider {
                data: (0..VALUES).collect(),
                partitions: N_PARTITIONS,
            }),
        )?;
        Ok(())
    });
    let mut driver = DataFusionDriver::new_with_context_init(None, init);
    if let Some(codec) = codec {
        driver = driver.with_codec(codec);
    }
    let database = driver.new_database().unwrap();
    database.new_connection().unwrap()
}

/// Read every `v` value produced by the descriptor.
fn read_values(connection: &DataFusionConnection, descriptor: &[u8]) -> Vec<i32> {
    let reader = connection.read_partition(descriptor).unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out
}

#[test]
fn proto_round_trips_custom_plan_without_replanning() {
    let mut planner = custom_connection(Some(Arc::new(CustomCodec)));
    let mut statement = planner.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();

    let result = statement.execute_partitions().unwrap();
    assert_eq!(
        result.partitions.len(),
        N_PARTITIONS,
        "one descriptor per custom output partition"
    );

    // A FRESH connection — also codec-equipped, as a separate executor would be — reproduces
    // the rows purely from the serialized plan (no SQL string rides the descriptor).
    let executor = custom_connection(Some(Arc::new(CustomCodec)));
    let mut values = Vec::new();
    for descriptor in &result.partitions {
        values.extend(read_values(&executor, descriptor));
    }
    values.sort();
    assert_eq!(values, (0..VALUES).collect::<Vec<_>>());
}

#[test]
fn custom_node_without_codec_errors() {
    // No codec: the default codec cannot encode the custom node, so execute_partitions fails
    // rather than silently falling back to a re-plan path.
    let mut connection = custom_connection(None);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();

    assert!(
        statement.execute_partitions().is_err(),
        "custom node + no codec must error, not fall back"
    );
}

#[test]
fn codec_encode_failure_errors() {
    // The codec can't encode this plan; execute_partitions errors the whole query (no
    // re-plan fallback).
    let mut connection = custom_connection(Some(Arc::new(FailingCodec)));
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();

    assert!(
        statement.execute_partitions().is_err(),
        "failed proto encode must error, not fall back"
    );
}

#[test]
fn read_partition_rejects_out_of_range_index() {
    let mut planner = custom_connection(Some(Arc::new(CustomCodec)));
    let mut statement = planner.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();
    let result = statement.execute_partitions().unwrap();

    // Corrupt the index (bytes 1..5) to an out-of-range partition.
    let mut bad = result.partitions[0].clone();
    bad[1..5].copy_from_slice(&u32::to_le_bytes(99));

    let executor = custom_connection(Some(Arc::new(CustomCodec)));
    let err = match executor.read_partition(&bad) {
        Ok(_) => panic!("expected out-of-range index to be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
}

#[test]
fn plan_cache_deserializes_once_across_partitions() {
    let decodes = Arc::new(AtomicUsize::new(0));
    let codec: PhysicalCodec = Arc::new(CountingCodec {
        decodes: decodes.clone(),
    });
    let mut connection = custom_connection(Some(codec));
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();
    let result = statement.execute_partitions().unwrap();
    assert_eq!(result.partitions.len(), N_PARTITIONS);

    // Read every partition on the SAME connection: the plan is identical across them, so
    // the cache should deserialize it once and reuse it for the rest.
    let mut values = Vec::new();
    for descriptor in &result.partitions {
        values.extend(read_values(&connection, descriptor));
    }
    values.sort();
    assert_eq!(values, (0..VALUES).collect::<Vec<_>>());

    assert_eq!(
        decodes.load(Ordering::SeqCst),
        1,
        "plan should be deserialized once and cached across partition indices"
    );
}
