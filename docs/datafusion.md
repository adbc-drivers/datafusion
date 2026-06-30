---
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# Apache DataFusion Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Apache DataFusion][datafusion].

:::{note}
This project is not part of the Apache Software Foundation.
:::

## Installation & Quickstart

The driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install datafusion
```

## Connecting

DataFusion is an in-process query engine and does not need a connection string or URI.

```python
from adbc_driver_manager import dbapi

dbapi.connect(driver="datafusion")
```

You can provide the URI `datafusion://`, but no other URI is currently accepted:

```python
from adbc_driver_manager import dbapi

dbapi.connect("datafusion://")
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.  See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

## Feature & Type Support

The DataFusion driver supports many of the extensions to the SQL dialect that the [DataFusion CLI](https://datafusion.apache.org/user-guide/cli/) implements, including `SHOW ALL`, `SHOW`, `SET <OPTION> TO <VALUE>`, `CREATE EXTERNAL TABLE`, and scanning local and remote files/directories, including over HTTP and on S3.

For example:

```sql
SELECT `Breed Name`, `Lifespan`
  FROM 'https://hyperparam-public.s3.amazonaws.com/bunnies.parquet'
  ORDER BY `Lifespan` DESC
  LIMIT 5;

-- Result:
-- ┌──────────────────┬──────────┐
-- │ Breed Name       │ Lifespan │
-- ├──────────────────┼──────────┤
-- │ French Angora    │ 12       │
-- │ English Angora   │ 10       │
-- │ Netherland Dwarf │ 10       │
-- │ Mini Lop         │ 9        │
-- │ Lionhead         │ 9        │
-- └──────────────────┴──────────┘
```

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Partitioned execution

ADBC partitioned execution splits a result set into independent pieces that can be read
in parallel. One caller runs `execute_partitions`, which plans the query and returns one
opaque **descriptor** per output partition; each descriptor is then handed to
`read_partition`, which produces that partition's rows. The two halves need not run in the
same place: descriptors can be read back on the same connection, on other threads, or
shipped to other processes or machines (for example, a distributed engine fanning each
descriptor out to a worker). A descriptor is self-contained — the only thing that travels
between the planner and the reader.

A descriptor carries the planner's physical plan, so a reader runs exactly the plan the
planner built — it needs no query text or session configuration, only the descriptor (plus
any providers and codec the plan references).

### Shuffling plans

Partitioned execution is fast when each output partition reads independent input. When the
plan **shuffles** (a hash repartition for a join or grouped aggregate, or a sort), every
output partition reads *all* input, so executing partitions independently re-runs the
pre-shuffle pipeline once per partition — usually slower than a single execution.

The statement option `datafusion.partition_mode` controls this:

| Value | Behavior |
| --- | --- |
| `auto` (default) | Collapse to a single partition when the plan shuffles, otherwise natural partitions. |
| `multi` | One descriptor per natural output partition; logs a warning if the plan shuffles. |
| `single` | Always collapse to a single partition. |

```rust
statement.set_option(
    OptionStatement::Other("datafusion.partition_mode".into()),
    OptionValue::String("multi".into()),
)?;
```

`single` delivers the same one-partition result as `execute`, through the partition API.

### Custom extensions

The pre-packaged driver exposes the built-in DataFusion engine. To add your own
[`TableProvider`], catalog, object store, UDFs, or custom `ExecutionPlan` nodes, build a
*derived driver*: a small Rust crate that depends on this one, bakes your extensions in, and
exports its own ADBC entrypoint as a `cdylib`. A driver manager then loads your shared
library exactly like any other ADBC driver — the Python examples above work unchanged once
`driver=` points at your `.so`/`.dylib`/`.dll`.

Two hooks customize the driver:

- **`new_with_context_init`** — runs a closure against each database's `SessionContext`. This
  is where you register table providers, catalogs, object stores, and UDFs.
- **`with_codec`** — registers a `PhysicalExtensionCodec`. Needed *only* when your plans
  contain custom physical-plan extensions (custom `ExecutionPlan` nodes, UDFs, exprs), so
  they round-trip into partition descriptors; without one, such a plan fails
  `execute_partitions`. A provider that emits only standard DataFusion plan nodes does not
  need a codec. See `tests/test_proto_partitions.rs` for a codec example.

The C entrypoint constructs the driver via `Default` (it takes no arguments), so bake your
extensions into a `Default` impl on a thin newtype. The `Driver` trait is just
`new_database` / `new_database_with_opts`, which delegate to the inner driver:

```rust
use std::sync::Arc;
use adbc_core::Driver;
use adbc_driver_datafusion::{DataFusionDatabase, DataFusionDriver};

struct MyDriver(DataFusionDriver);

impl Default for MyDriver {
    fn default() -> Self {
        // Register custom extensions on every database's SessionContext.
        let context_init = Arc::new(|ctx: &mut datafusion::prelude::SessionContext, _opts| {
            ctx.register_table("my_table", Arc::new(MyTableProvider::new()))?;
            Ok(())
        });

        let driver = DataFusionDriver::new_with_context_init(None, context_init)
            .with_codec(Arc::new(MyCodec::default())); // only for custom plan nodes

        MyDriver(driver)
    }
}

impl Driver for MyDriver {
    type DatabaseType = DataFusionDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        self.0.new_database()
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<
            Item = (adbc_core::options::OptionDatabase, adbc_core::options::OptionValue),
        >,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        self.0.new_database_with_opts(opts)
    }
}

adbc_ffi::export_driver!(MyDatafusionDriverInit, MyDriver);
```

Build it with `crate-type = ["cdylib"]` and load your entrypoint:

```python
from adbc_driver_manager import dbapi

dbapi.connect(
    driver="/path/to/libmy_datafusion_driver.so",
    entrypoint="MyDatafusionDriverInit",
)
```

Because extensions are Rust types compiled into the driver, this derived-driver approach is
the supported path for custom providers and codecs — there is no way to inject them through
a driver manager's options at load time. Ordinary partitioned execution over built-in and
registered sources works through any driver manager without a derived driver, since
descriptors are opaque bytes the manager simply hands back to `read_partition`.

[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.24.1](./v0.24.1.md)

[datafusion]: https://datafusion.apache.org/
