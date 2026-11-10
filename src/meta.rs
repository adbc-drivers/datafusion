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

// Helpers for implementing metadata collections.

use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use datafusion::execution::context::SessionContext;
use driverbase::{
    error::ErrorHelper as _,
    meta::{
        Filter, FilterDescription, MetadataCollection, MetadataCollectionQuery, Result,
        SupportedMetadataCollections,
    },
};

use super::ErrorHelper;

struct CatalogCollection;

impl MetadataCollection<Arc<SessionContext>, ErrorHelper> for CatalogCollection {
    fn name(&self) -> &'static str {
        "catalogs"
    }

    fn remarks(&self) -> Option<String> {
        Some("The catalogs defined in the database".to_string())
    }

    fn schema(&self) -> Schema {
        driverbase::meta::schema::CATALOG.clone()
    }

    fn supported_filters(&self) -> Vec<FilterDescription> {
        vec![
            FilterDescription {
                name: "catalog".to_string(),
                filter_type_string: true,
                remarks: Some("The catalog name to filter by".to_string()),
                ..Default::default()
            },
            FilterDescription {
                name: "catalog".to_string(),
                filter_type_string: true,
                is_pattern: true,
                ..Default::default()
            },
        ]
    }

    fn init(&self) -> Box<dyn MetadataCollectionQuery<Arc<SessionContext>, ErrorHelper> + Send> {
        Box::new(CatalogCollectionQuery { catalog: None })
    }
}

struct CatalogCollectionQuery {
    catalog: Option<Filter>,
}

#[async_trait::async_trait]
impl MetadataCollectionQuery<Arc<SessionContext>, ErrorHelper> for CatalogCollectionQuery {
    fn add_filter(&mut self, name: &str, value: Filter) -> Result<(), ErrorHelper> {
        match name {
            "catalog" => {
                self.catalog = Some(value);
                Ok(())
            }
            _ => Err(ErrorHelper::invalid_argument().format(format_args!(
                "unsupported filter {name} for metadata collection 'catalogs'"
            ))),
        }
    }

    async fn execute(
        &self,
        ctx: Arc<SessionContext>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>, ErrorHelper> {
        let mut catalogs = ctx.catalog_names();
        catalogs.sort();

        let mut catalog_name = arrow_array::builder::StringBuilder::new();
        let mut catalog_remarks = arrow_array::builder::StringBuilder::new();
        catalog_remarks.append_nulls(catalogs.len());

        for name in catalogs {
            catalog_name.append_value(name);
        }

        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(driverbase::meta::schema::CATALOG.clone()),
            vec![
                Arc::new(catalog_name.finish()),
                Arc::new(catalog_remarks.finish()),
            ],
        )?;

        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            Arc::new(driverbase::meta::schema::CATALOG.clone()),
        );

        Ok(Box::new(reader))
    }
}

struct SchemaCollection;

impl MetadataCollection<Arc<SessionContext>, ErrorHelper> for SchemaCollection {
    fn name(&self) -> &'static str {
        "schemas"
    }

    fn remarks(&self) -> Option<String> {
        Some("The schemas defined in the database".to_string())
    }

    fn schema(&self) -> Schema {
        driverbase::meta::schema::SCHEMA.clone()
    }

    fn supported_filters(&self) -> Vec<FilterDescription> {
        vec![
            FilterDescription {
                name: "catalog".to_string(),
                filter_type_string: true,
                remarks: Some("The catalog name to filter by".to_string()),
                ..Default::default()
            },
            FilterDescription {
                name: "catalog".to_string(),
                filter_type_string: true,
                is_pattern: true,
                ..Default::default()
            },
            FilterDescription {
                name: "schema".to_string(),
                filter_type_string: true,
                remarks: Some("The schema name to filter by".to_string()),
                ..Default::default()
            },
            FilterDescription {
                name: "schema".to_string(),
                filter_type_string: true,
                is_pattern: true,
                ..Default::default()
            },
        ]
    }

    fn init(&self) -> Box<dyn MetadataCollectionQuery<Arc<SessionContext>, ErrorHelper> + Send> {
        Box::new(SchemaCollectionQuery::default())
    }
}

#[derive(Default)]
struct SchemaCollectionQuery {
    catalog: Option<Filter>,
    schema: Option<Filter>,
}

#[async_trait::async_trait]
impl MetadataCollectionQuery<Arc<SessionContext>, ErrorHelper> for SchemaCollectionQuery {
    fn add_filter(&mut self, name: &str, value: Filter) -> Result<(), ErrorHelper> {
        match name {
            "schema" => {
                self.schema = Some(value);
                Ok(())
            }
            _ => Err(ErrorHelper::invalid_argument().format(format_args!(
                "unsupported filter {name} for metadata collection 'schemas'"
            ))),
        }
    }

    async fn execute(
        &self,
        ctx: Arc<SessionContext>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>, ErrorHelper> {
        let mut catalogs = ctx.catalog_names();
        catalogs.sort();

        let mut catalog_name = arrow_array::builder::StringBuilder::new();
        let mut schema_name = arrow_array::builder::StringBuilder::new();
        let mut schema_remarks = arrow_array::builder::StringBuilder::new();

        for name in catalogs {
            if let Some(filter) = &self.catalog {
                if !filter.matches(&name) {
                    continue;
                }
            }
            if let Some(catalog) = ctx.catalog(&name) {
                for schema in catalog.schema_names() {
                    if let Some(filter) = &self.schema {
                        if !filter.matches(&schema) {
                            continue;
                        }
                    }
                    catalog_name.append_value(&name);
                    schema_name.append_value(&schema);
                    schema_remarks.append_null();
                }
            }
            // else TOC/TOU error?
        }

        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(driverbase::meta::schema::SCHEMA.clone()),
            vec![
                Arc::new(catalog_name.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(schema_remarks.finish()),
            ],
        )?;

        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            Arc::new(driverbase::meta::schema::SCHEMA.clone()),
        );

        Ok(Box::new(reader))
    }
}

pub static SUPPORTED_METADATA_COLLECTIONS: LazyLock<
    SupportedMetadataCollections<Arc<SessionContext>, ErrorHelper>,
> = LazyLock::new(|| {
    let mut collections: std::collections::HashMap<
        &'static str,
        Box<dyn MetadataCollection<Arc<SessionContext>, ErrorHelper>>,
    > = std::collections::HashMap::new();
    collections.insert("catalogs", Box::new(CatalogCollection));
    collections.insert("schemas", Box::new(SchemaCollection));
    SupportedMetadataCollections::new(collections)
});
