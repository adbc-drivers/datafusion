<!---
  Copyright (c) 2025 ADBC Drivers Contributors

  This file has been modified from its original version, which is
  under the Apache License:

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ADBC Driver for Apache DataFusion

![Vendor: Apache DataFusion](https://img.shields.io/badge/vendor-Apache%20DataFusion-blue?style=flat-square)
![Implementation: Rust](https://img.shields.io/badge/implementation-Rust-violet?style=flat-square)
![Status: Experimental](https://img.shields.io/badge/status-experimental-red?style=flat-square)

Not affiliated with the Apache Software Foundation.

An [ADBC driver](https://arrow.apache.org/adbc/) for Apache DataFusion.

## Installation

Pre-packaged builds of the drivers in this repo will be made available for
various platforms from the [Columnar](https://columnar.tech) CDN in the future.

See [Building](#building) to build the drivers yourself.

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

### Custom nodes

A provider that emits custom `ExecutionPlan` nodes must register a `PhysicalExtensionCodec`
so those nodes round-trip into descriptors; without one, a plan containing a custom node
fails `execute_partitions`. See `tests/test_proto_partitions.rs` for a worked example.

```rust
let driver = DataFusionDriver::new_with_context_init(handle, context_init).with_codec(codec);
```

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
