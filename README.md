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

`execute_partitions` serializes the already-built **physical plan** into each descriptor
([`datafusion-proto`](https://docs.rs/datafusion-proto)), and `read_partition`
**deserializes** it and executes one partition. Because the reader runs the exact plan the
planner built, every reader agrees on the partition layout and the reader needs only the
plan bytes (plus any registered providers and codec) — not the original query text or the
planner's session configuration.

### Custom nodes

`datafusion-proto` serializes built-in nodes with no configuration. A provider that emits
custom `ExecutionPlan` nodes must register a `PhysicalExtensionCodec` alongside the
`ContextInit` hook so those nodes round-trip:

```rust
let driver = DataFusionDriver::new_with_context_init(handle, context_init).with_codec(codec);
```

The custom node must be reconstructable from its encoded bytes plus the hook-built session.
With no codec the driver uses the default codec, which covers built-in nodes only; a plan
containing a custom node the codec cannot encode **fails** `execute_partitions` rather than
falling back to a re-plan path.

`read_partition` caches a deserialized plan per connection, keyed by the descriptor's
plan bytes, so a connection that reads several partitions of the same plan pays the
decode cost once per distinct plan rather than once per partition.

See `tests/test_proto_partitions.rs` for a worked custom provider + codec example.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
