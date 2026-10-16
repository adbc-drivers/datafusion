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
use driverbase::error::ErrorHelper as _;

use super::ErrorHelper;

type Result<T> = std::result::Result<T, super::DriverError>;

#[derive(Clone)]
pub struct SupportedMetadataCollections<Args> {
    collections: Arc<std::collections::HashMap<&'static str, Box<dyn MetadataCollection<Args>>>>,
}

// Store metadata collection-related options on a Statement.
pub struct MetadataCollectionState<Args> {
    collection: Option<Box<dyn MetadataCollectionQuery<Args>>>,
    supported_collections: SupportedMetadataCollections<Args>,
}

impl<Args> MetadataCollectionState<Args> {
    pub fn new(supported_collections: SupportedMetadataCollections<Args>) -> Self {
        Self {
            collection: None,
            supported_collections,
        }
    }

    pub fn clear(&mut self) {
        self.collection = None;
    }

    pub fn is_set(&self) -> bool {
        self.collection.is_some()
    }

    pub fn take(&mut self) -> Option<Box<dyn MetadataCollectionQuery<Args>>> {
        self.collection.take()
    }

    pub fn set_option(
        &mut self,
        key: &adbc_core::options::OptionStatement,
        value: &adbc_core::options::OptionValue,
    ) -> Result<bool> {
        if let (
            adbc_core::options::OptionStatement::Other(k),
            adbc_core::options::OptionValue::String(v),
        ) = (key, value)
        {
            if k == "adbc.metadata.collection" {
                if v == "meta" {
                    // TODO:
                    todo!()
                } else if let Some(collection) =
                    self.supported_collections.collections.get(v.as_str())
                {
                    self.collection = Some(collection.init());
                    Ok(true)
                } else {
                    Err(ErrorHelper::invalid_argument().format(format_args!(
                        "unknown collection adbc.metadata.collection={v:?}"
                    )))
                }
            } else if let Some(name) = k.strip_prefix("adbc.metadata.filter_pattern.") {
                if let Some(collection) = &mut self.collection {
                    collection.add_filter(name, Filter::Pattern(v.clone()))?;
                    Ok(true)
                } else {
                    Err(ErrorHelper::invalid_argument()
                        .format(format_args!("filter {k} specified before collection")))
                }
            } else if let Some(name) = k.strip_prefix("adbc.metadata.filter.") {
                if let Some(collection) = &mut self.collection {
                    collection.add_filter(name, Filter::Exact(v.clone()))?;
                    Ok(true)
                } else {
                    Err(ErrorHelper::invalid_argument()
                        .format(format_args!("filter {k}={v:?} specified before collection")))
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

static CATALOG_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        arrow_schema::Field::new("catalog_name", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new("catalog_remarks", arrow_schema::DataType::Utf8, true),
    ])
});

#[async_trait::async_trait]
pub trait MetadataCollectionQuery<Args> {
    fn add_filter(&mut self, name: &str, filter: Filter) -> Result<()>;
    async fn execute(&self, args: Args) -> Result<Box<dyn RecordBatchReader + Send + 'static>>;
}

pub trait MetadataCollection<Args>: Send + Sync {
    fn name(&self) -> &'static str;
    fn init(&self) -> Box<dyn MetadataCollectionQuery<Args>>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Filter {
    Pattern(String),
    Exact(String),
}

impl Filter {
    fn matches(&self, value: &str) -> bool {
        match self {
            // TODO: we should store compiled regex
            Filter::Pattern(pattern) => pattern == value,
            Filter::Exact(exact) => exact == value,
        }
    }
}

struct CatalogCollection;

impl MetadataCollection<Arc<SessionContext>> for CatalogCollection {
    fn name(&self) -> &'static str {
        "catalogs"
    }

    fn init(&self) -> Box<dyn MetadataCollectionQuery<Arc<SessionContext>>> {
        Box::new(CatalogCollectionQuery { catalog: None })
    }
}

struct CatalogCollectionQuery {
    catalog: Option<Filter>,
}

#[async_trait::async_trait]
impl MetadataCollectionQuery<Arc<SessionContext>> for CatalogCollectionQuery {
    fn add_filter(&mut self, name: &str, value: Filter) -> Result<()> {
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
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut catalogs = ctx.catalog_names();
        catalogs.sort();

        let mut catalog_name = arrow_array::builder::StringBuilder::new();
        let mut catalog_remarks = arrow_array::builder::StringBuilder::new();
        catalog_remarks.append_nulls(catalogs.len());

        for name in catalogs {
            catalog_name.append_value(name);
        }

        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(CATALOG_SCHEMA.clone()),
            vec![
                Arc::new(catalog_name.finish()),
                Arc::new(catalog_remarks.finish()),
            ],
        )?;

        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            Arc::new(CATALOG_SCHEMA.clone()),
        );

        Ok(Box::new(reader))
    }
}

pub static SUPPORTED_METADATA_COLLECTIONS: LazyLock<
    SupportedMetadataCollections<Arc<SessionContext>>,
> = LazyLock::new(|| {
    let mut collections: std::collections::HashMap<
        &'static str,
        Box<dyn MetadataCollection<Arc<SessionContext>>>,
    > = std::collections::HashMap::new();
    collections.insert("catalogs", Box::new(CatalogCollection));
    SupportedMetadataCollections {
        collections: Arc::new(collections),
    }
});
