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

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.ingest
import pyarrow
import pytest

from . import datafusion


def pytest_generate_tests(metafunc) -> None:
    quirks = datafusion.get_quirks(metafunc.config.getoption("vendor_version"))
    return adbc_drivers_validation.tests.ingest.generate_tests([quirks], metafunc)


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    pass


@pytest.mark.requires_features(
    [
        "statement_bulk_ingest",
        "statement_bulk_ingest_schema",
        "statement_bulk_ingest_catalog",
    ]
)
@pytest.mark.parametrize("mode", ["append", "create_append"])
@pytest.mark.parametrize(
    "target,decoy",
    [
        pytest.param(
            (None, None, "secondary.ingest_target"),
            (None, "secondary", "ingest_target"),
            id="bare-dotted-collision",
        ),
        pytest.param(
            (None, "secondary", "public.ingest_target"),
            ("secondary", "public", "ingest_target"),
            id="schema-qualified-dotted-collision",
        ),
        *[
            pytest.param(
                (catalog, schema, "IngestTarget"),
                (catalog, schema, "ingesttarget"),
                id=f"{kind}-mixed-case-table-collision",
            )
            for kind, catalog, schema in [
                ("bare", None, None),
                ("schema", None, "secondary"),
                ("full", "secondary_catalog", "secondary_schema"),
            ]
        ],
        *[
            pytest.param(
                (catalog, schema, name),
                None,
                id=f"{kind}-{label}",
            )
            for kind, catalog, schema in [
                ("bare", None, None),
                ("schema", None, "secondary"),
                ("full", "secondary_catalog", "secondary_schema"),
            ]
            for label, name in [
                ("quote", 'ingest"target'),
                ("space", "ingest target"),
                ("reserved", "select"),
                ("dot", "ingest.target"),
            ]
        ],
        pytest.param(
            (None, "IngestSchema", "ingest_target"),
            (None, "ingestschema", "ingest_target"),
            id="schema-mixed-case-collision",
        ),
        pytest.param(
            ("IngestCatalog", "public", "ingest_target"),
            ("ingestcatalog", "public", "ingest_target"),
            id="catalog-mixed-case-collision",
        ),
        pytest.param(
            ("ingest catalog", 'ingest" schema', "ingest_target"),
            None,
            id="unusual-catalog-and-schema",
        ),
        pytest.param(
            ('ingest" catalog', "select", "ingest_target"),
            None,
            id="quoted-catalog-reserved-schema",
        ),
    ],
)
def test_append_preserves_table_reference(driver, driver_path, mode, target, decoy):
    def quote(identifier):
        return '"' + identifier.replace('"', '""') + '"'

    def sql_reference(reference):
        return ".".join(quote(part) for part in reference if part is not None)

    def ingest(connection, reference, values, ingest_mode):
        catalog, schema, table = reference
        with connection.cursor() as ingest_cursor:
            return ingest_cursor.adbc_ingest(
                table,
                pyarrow.table({"value": pyarrow.array(values, type=pyarrow.int64())}),
                mode=ingest_mode,
                catalog_name=catalog,
                db_schema_name=schema,
            )

    # Use a separate database for each case for isolation
    with adbc_driver_manager.dbapi.connect(
        driver=driver_path, autocommit=True
    ) as connection:
        with connection.cursor() as cursor:
            for reference, values in [(target, [10]), (decoy, [100])]:
                if reference is None:
                    continue
                catalog, schema, _ = reference
                if catalog is not None:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {quote(catalog)}")
                if schema is not None:
                    namespace = sql_reference((catalog, schema))
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
                assert ingest(connection, reference, values, "create") == 1

            assert ingest(connection, target, [20, 30], mode) == 2
            cursor.execute(f"SELECT value FROM {sql_reference(target)} ORDER BY value")
            assert cursor.fetchall() == [(10,), (20,), (30,)]
            if decoy is not None:
                cursor.execute(
                    f"SELECT value FROM {sql_reference(decoy)} ORDER BY value"
                )
                assert cursor.fetchall() == [(100,)]
