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

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use regex::Regex;

use driverbase::error::ErrorHelper as _;
use driverbase::get_objects::{ColumnInfo, GetObjectsImpl, TableAndColumnInfo, TableInfo};

use crate::{DriverError, ErrorHelper, Runtime};

fn like_to_regex(pattern: &str) -> Result<Regex, DriverError> {
    let mut re = String::with_capacity(pattern.len() + 2);
    re.push('^');
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(escaped) = chars.next() {
                    re.push_str(&regex::escape(&escaped.to_string()));
                }
            }
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push('$');
    Regex::new(&re).map_err(|e| {
        ErrorHelper::invalid_argument()
            .context("convert LIKE pattern to regex")
            .message(e.to_string())
    })
}

pub(crate) struct DataFusionGetObjects {
    ctx: Arc<SessionContext>,
    runtime: Arc<Runtime>,
}

impl DataFusionGetObjects {
    pub(crate) fn new(ctx: Arc<SessionContext>, runtime: Arc<Runtime>) -> Self {
        Self { ctx, runtime }
    }

    fn query(&self, sql: &str, params: Vec<ScalarValue>) -> Result<Vec<RecordBatch>, DriverError> {
        self.runtime.block_on(async {
            let df = self
                .ctx
                .sql(sql)
                .await
                .map_err(ErrorHelper::from_datafusion)?;
            let df = df
                .with_param_values(params)
                .map_err(ErrorHelper::from_datafusion)?;
            df.collect().await.map_err(ErrorHelper::from_datafusion)
        })
    }

    fn string_column(batch: &RecordBatch, index: usize) -> Result<&StringArray, DriverError> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ErrorHelper::internal(driverbase::location!())
                    .message("expected StringArray column in information_schema result")
            })
    }
}

impl GetObjectsImpl<ErrorHelper> for DataFusionGetObjects {
    fn get_catalogs(&self, filter: Option<&str>) -> Result<Vec<String>, DriverError> {
        let mut catalogs = self.ctx.catalog_names();
        catalogs.sort();
        if let Some(pattern) = filter {
            let re = like_to_regex(pattern)?;
            catalogs.retain(|name| re.is_match(name));
        }
        Ok(catalogs)
    }

    fn get_db_schemas(
        &self,
        catalog: &str,
        filter: Option<&str>,
    ) -> Result<Vec<String>, DriverError> {
        let (sql, params) = match filter {
            Some(pattern) => (
                "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = $1 AND schema_name LIKE $2 ORDER BY schema_name",
                vec![ScalarValue::from(catalog), ScalarValue::from(pattern)],
            ),
            None => (
                "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = $1 ORDER BY schema_name",
                vec![ScalarValue::from(catalog)],
            ),
        };

        let batches = self.query(sql, params)?;
        let mut schemas = Vec::new();
        for batch in &batches {
            let col = Self::string_column(batch, 0)?;
            for i in 0..col.len() {
                if !col.is_null(i) {
                    schemas.push(col.value(i).to_string());
                }
            }
        }
        Ok(schemas)
    }

    fn get_tables(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
    ) -> Result<Vec<TableInfo>, DriverError> {
        let mut sql = String::from(
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = $1 AND table_schema = $2",
        );
        let mut params: Vec<ScalarValue> =
            vec![ScalarValue::from(catalog), ScalarValue::from(db_schema)];
        let mut param_idx = 3;

        if let Some(pattern) = table_filter {
            sql.push_str(&format!(" AND table_name LIKE ${param_idx}"));
            params.push(ScalarValue::from(pattern));
            param_idx += 1;
        }

        if let Some(types) = table_type_filter
            && !types.is_empty()
        {
            let placeholders: Vec<String> = types
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", param_idx + i))
                .collect();
            sql.push_str(&format!(" AND table_type IN ({})", placeholders.join(", ")));
            for t in types {
                params.push(ScalarValue::from(t.as_str()));
            }
        }

        sql.push_str(" ORDER BY table_name");

        let batches = self.query(&sql, params)?;
        let mut tables = Vec::new();
        for batch in &batches {
            let names = Self::string_column(batch, 0)?;
            let types = Self::string_column(batch, 1)?;
            for i in 0..batch.num_rows() {
                tables.push(TableInfo {
                    table_name: names.value(i).to_string(),
                    table_type: types.value(i).to_string(),
                });
            }
        }
        Ok(tables)
    }

    fn get_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
        column_filter: Option<&str>,
    ) -> Result<Vec<TableAndColumnInfo>, DriverError> {
        let mut sql = String::from(
            "SELECT c.table_name, t.table_type, c.column_name \
             FROM information_schema.columns c \
             JOIN information_schema.tables t \
               ON c.table_catalog = t.table_catalog \
               AND c.table_schema = t.table_schema \
               AND c.table_name = t.table_name \
             WHERE c.table_catalog = $1 AND c.table_schema = $2",
        );
        let mut params: Vec<ScalarValue> =
            vec![ScalarValue::from(catalog), ScalarValue::from(db_schema)];
        let mut param_idx = 3;

        if let Some(pattern) = table_filter {
            sql.push_str(&format!(" AND c.table_name LIKE ${param_idx}"));
            params.push(ScalarValue::from(pattern));
            param_idx += 1;
        }

        if let Some(types) = table_type_filter
            && !types.is_empty()
        {
            let placeholders: Vec<String> = types
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", param_idx + i))
                .collect();
            sql.push_str(&format!(
                " AND t.table_type IN ({})",
                placeholders.join(", ")
            ));
            for t in types {
                params.push(ScalarValue::from(t.as_str()));
            }
            param_idx += types.len();
        }

        if let Some(pattern) = column_filter {
            sql.push_str(&format!(" AND c.column_name LIKE ${param_idx}"));
            params.push(ScalarValue::from(pattern));
        }

        sql.push_str(" ORDER BY c.table_name, c.ordinal_position");

        let batches = self.query(&sql, params)?;

        let mut result: Vec<TableAndColumnInfo> = Vec::new();
        for batch in &batches {
            let table_names = Self::string_column(batch, 0)?;
            let table_types = Self::string_column(batch, 1)?;
            let column_names = Self::string_column(batch, 2)?;

            for i in 0..batch.num_rows() {
                let tname = table_names.value(i);
                let ttype = table_types.value(i);
                let cname = column_names.value(i);

                let should_append = match result.last() {
                    Some(last) => last.table.table_name != tname,
                    None => true,
                };

                if should_append {
                    result.push(TableAndColumnInfo {
                        table: TableInfo {
                            table_name: tname.to_string(),
                            table_type: ttype.to_string(),
                        },
                        columns: Vec::new(),
                    });
                }

                // Safe: we just pushed if should_append was true, so last_mut always succeeds
                if let Some(last) = result.last_mut() {
                    last.columns.push(ColumnInfo {
                        column_name: cname.to_string(),
                    });
                }
            }
        }
        Ok(result)
    }
}
