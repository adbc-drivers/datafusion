# Copyright (c) 2025 ADBC Drivers Contributors
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

import functools
from pathlib import Path

from adbc_drivers_validation import model, quirks


class DataFusionQuirks(model.DriverQuirks):
    name = "datafusion"
    driver = "adbc_driver_datafusion"
    driver_name = "ADBC Driver Foundry Driver for DataFusion"
    vendor_name = "DataFusion"
    vendor_version = "25.12"
    short_version = "25.12"
    features = model.DriverFeatures(
        statement_bind=False,
        current_catalog="datafusion",
        current_schema="public",
        get_objects=True,
        statement_bulk_ingest=True,
        statement_rows_affected=True,
    )
    setup = model.DriverSetup(
        database={},
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        msg = str(error)
        if table_name and table_name not in msg:
            return False
        return "Not found: Table" in msg or "does not exist" in msg

    def quote_one_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    # @property
    # def sample_ddl_constraints(self) -> list[str]:
    #     return [
    #         "CREATE TABLE constraint_primary (z INT, a INT, b STRING, PRIMARY KEY (a) NOT ENFORCED)",
    #         "CREATE TABLE constraint_primary_multi (z INT, a INT, b STRING, PRIMARY KEY (b, a) NOT ENFORCED)",
    #         "CREATE TABLE constraint_primary_multi2 (z INT, a STRING, b INT, PRIMARY KEY (a, b) NOT ENFORCED)",
    #         "CREATE TABLE constraint_foreign (z INT, a INT, b INT, FOREIGN KEY (b) REFERENCES constraint_primary(a) NOT ENFORCED)",
    #         "CREATE TABLE constraint_foreign_multi (z INT, a INT, b INT, c STRING, FOREIGN KEY (c, b) REFERENCES constraint_primary_multi2(a, b) NOT ENFORCED)",
    #         # Ensure the driver doesn't misinterpret column IDs as indices
    #         "ALTER TABLE constraint_primary DROP COLUMN z",
    #         "ALTER TABLE constraint_primary_multi DROP COLUMN z",
    #         "ALTER TABLE constraint_primary_multi2 DROP COLUMN z",
    #         "ALTER TABLE constraint_foreign DROP COLUMN z",
    #         "ALTER TABLE constraint_foreign_multi DROP COLUMN z",
    #     ]

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)


@functools.cache
def get_quirks(version: str) -> model.DriverQuirks:
    if version == "25.12":
        return DataFusionQuirks()
    raise ValueError(f"unsupported version: {version}")
