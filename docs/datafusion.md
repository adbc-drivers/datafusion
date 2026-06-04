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

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.24.1](./v0.24.1.md)

[datafusion]: https://datafusion.apache.org/
