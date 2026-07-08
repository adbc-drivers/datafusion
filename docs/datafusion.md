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

### Partitioned Execution

This driver supports ADBC's partitioned execution; one partition is generated per output partition. Each partition contains the physical plan, and can be distributed across CPUs or physical machines. If the plan contains a shuffle, however, distributed execution will likely be slower, as each partition must read the entire output.

The statement option `datafusion.partition_mode` controls what to do if a shuffle is detected:

| Value            | Behavior                                                                            |
|------------------|-------------------------------------------------------------------------------------|
| `auto` (default) | Collapse to a single partition when the plan shuffles, otherwise natural partitions |
| `multi`          | One descriptor per natural output partition; logs a warning if the plan shuffles    |
| `single`         | Always collapse to a single partition (effectively the same as regular execution)   |

### Extending DataFusion

It is possible to customize the embedded DataFusion by depending on this crate, then using hooks to customize the DataFusion `SessionContext` and register `PhysicalExtensionCodec`s. The customized driver can then be built into a shared library and distributed as an ADBC driver. For more, see:

- `DataFusionDriver::new_with_context_init`, which accepts a callback to modify the `SessionContext`
- `DataFusionDriver::with_codec`, which accepts a `PhysicalExtensionCodec` used to serialize/deserialize plans in partitioned execution
- The [`adbc_ffi`](https://crates.io/crates/adbc_ffi) crate, which provides helpers to export a Rust ADBC driver as a shared library

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.26.0](./v0.26.0.md)
- [v0.25.0](./v0.25.0.md)
- [v0.24.1](./v0.24.1.md)

[datafusion]: https://datafusion.apache.org/
