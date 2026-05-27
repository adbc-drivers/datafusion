// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use futures::StreamExt;

use crate::{ErrorHelper, Runtime};

pub fn row_to_scalar_values(
    batch: &RecordBatch,
    row_index: usize,
) -> adbc_core::error::Result<Vec<ScalarValue>> {
    let mut values = Vec::with_capacity(batch.num_columns());
    for col_index in 0..batch.num_columns() {
        let array = batch.column(col_index);
        let scalar =
            ScalarValue::try_from_array(array, row_index).map_err(ErrorHelper::from_datafusion)?;
        values.push(scalar);
    }
    Ok(values)
}

pub struct BindReader {
    runtime: Arc<Runtime>,
    ctx: Arc<SessionContext>,
    plan: LogicalPlan,
    bound: Box<dyn RecordBatchReader + Send>,
    current_batch: Option<RecordBatch>,
    next_row: usize,
    pending_results: Option<datafusion::execution::SendableRecordBatchStream>,
    schema: SchemaRef,
}

impl BindReader {
    pub fn new(
        runtime: Arc<Runtime>,
        ctx: Arc<SessionContext>,
        plan: LogicalPlan,
        bound: Box<dyn RecordBatchReader + Send>,
    ) -> Self {
        let schema: SchemaRef = plan.schema().as_arrow().clone().into();
        Self {
            runtime,
            ctx,
            plan,
            bound,
            current_batch: None,
            next_row: 0,
            pending_results: None,
            schema,
        }
    }

    fn advance(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        loop {
            if let Some(stream) = &mut self.pending_results {
                let maybe_batch = self.runtime.block_on(async { stream.next().await });
                match maybe_batch {
                    Some(Ok(batch)) => return Some(Ok(batch)),
                    Some(Err(e)) => return Some(Err(e.into())),
                    None => {
                        self.pending_results = None;
                        continue;
                    }
                }
            }
            self.pending_results = None;

            let batch = loop {
                if let Some(b) = &self.current_batch
                    && self.next_row < b.num_rows()
                {
                    break b;
                }

                match self.bound.next() {
                    Some(Ok(b)) => {
                        self.current_batch = Some(b);
                        self.next_row = 0;
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                }
            };

            let params = match row_to_scalar_values(batch, self.next_row) {
                Ok(p) => p,
                Err(e) => {
                    return Some(Err(ArrowError::ExternalError(Box::new(e))));
                }
            };
            self.next_row += 1;

            let result = self.runtime.block_on(async {
                let plan_with_params = self
                    .plan
                    .clone()
                    .with_param_values(params)
                    .map_err(ErrorHelper::from_datafusion)?;
                let df = self
                    .ctx
                    .execute_logical_plan(plan_with_params)
                    .await
                    .map_err(ErrorHelper::from_datafusion)?;
                df.execute_stream()
                    .await
                    .map_err(ErrorHelper::from_datafusion)
            });

            match result {
                Ok(batches) => {
                    self.pending_results = Some(batches);
                }
                Err(e) => {
                    return Some(Err(ArrowError::ExternalError(Box::new(e.to_adbc()))));
                }
            }
        }
    }
}

impl Iterator for BindReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.advance()
    }
}

impl RecordBatchReader for BindReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
