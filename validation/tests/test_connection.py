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
import adbc_drivers_validation.tests.connection as connection_tests
import pyarrow

from . import datafusion


def pytest_generate_tests(metafunc) -> None:
    quirks = datafusion.get_quirks(metafunc.config.getoption("vendor_version"))
    return connection_tests.generate_tests([quirks], metafunc)


class TestConnection(connection_tests.TestConnection):
    def test_meta_collection_meta(self, driver, conn) -> None:
        collection_schema = pyarrow.schema(
            [
                ("collection_name", pyarrow.string(), False),
                ("collection_remarks", pyarrow.string(), True),
            ]
        )

        with conn.cursor() as cursor:
            cursor.adbc_statement.set_options(
                **{
                    "adbc.metadata.collection": "meta",
                }
            )

            filters = pyarrow.schema(cursor.adbc_statement.get_parameter_schema())
            assert len(filters) == 0

            schema = pyarrow.schema(cursor.adbc_statement.execute_schema())
            assert schema == collection_schema

            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all().to_pylist()
                result.sort(key=lambda x: x["collection_name"])
                collections = [row["collection_name"] for row in result]
                assert collections == ["catalogs", "meta", "schemas"]

    def test_meta_collection_catalogs(self, driver, conn) -> None:
        collection_schema = pyarrow.schema(
            [
                ("catalog_name", pyarrow.string(), True),
                ("catalog_remarks", pyarrow.string(), True),
            ]
        )
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_options(
                **{
                    "adbc.metadata.collection": "catalogs",
                }
            )

            filters = pyarrow.schema(cursor.adbc_statement.get_parameter_schema())
            assert len(filters) == 2

            schema = pyarrow.schema(cursor.adbc_statement.execute_schema())
            assert schema == collection_schema

            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
                assert result.schema == collection_schema
                catalogs = result[0].to_pylist()
                assert sorted(catalogs) == catalogs
                assert driver.features.current_catalog in catalogs
                assert driver.features.secondary_catalog in catalogs

            cursor.adbc_statement.set_options(
                **{
                    "adbc.metadata.collection": "catalogs",
                }
            )
            filters = pyarrow.record_batch(
                [["secondary_catalog"]],
                schema=pyarrow.schema(
                    [
                        ("adbc.metadata.filter.catalog", pyarrow.string(), True),
                    ]
                ),
            )
            cursor.adbc_statement.bind(filters)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
                assert result.schema == collection_schema
                catalogs = result[0].to_pylist()
                assert catalogs == ["secondary_catalog"]

            cursor.adbc_statement.set_options(
                **{
                    "adbc.metadata.collection": "catalogs",
                }
            )
            filters = pyarrow.record_batch(
                [
                    ["secondary_catalog", None, "datafusion"],
                    [None, "secondary%", None],
                ],
                schema=pyarrow.schema(
                    [
                        ("adbc.metadata.filter.catalog", pyarrow.string(), True),
                        (
                            "adbc.metadata.filter_pattern.catalog",
                            pyarrow.string(),
                            True,
                        ),
                    ]
                ),
            )
            cursor.adbc_statement.bind(filters)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
                assert result.schema == collection_schema
                catalogs = result[0].to_pylist()
                assert catalogs == [
                    "secondary_catalog",
                    "secondary_catalog",
                    "datafusion",
                ]

    def test_meta_collection_schemas(self, driver, conn) -> None:
        collection_schema = pyarrow.schema(
            [
                ("catalog_name", pyarrow.string(), True),
                ("db_schema_name", pyarrow.string(), False),
                ("db_schema_remarks", pyarrow.string(), True),
            ]
        )
        filter_columns = [
            "adbc.metadata.filter.catalog",
            "adbc.metadata.filter_pattern.catalog",
            "adbc.metadata.filter.schema",
            "adbc.metadata.filter_pattern.schema",
        ]
        current = (driver.features.current_catalog, driver.features.current_schema)
        secondary = (
            driver.features.secondary_catalog,
            driver.features.secondary_catalog_schema,
        )
        with conn.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_options(**{"adbc.metadata.collection": "schemas"})
            filter_schema = pyarrow.schema(statement.get_parameter_schema())
            assert filter_schema == pyarrow.schema(
                [(name, pyarrow.string(), True) for name in filter_columns]
            )
            assert pyarrow.schema(statement.execute_schema()) == collection_schema

            handle, _ = statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
            assert result.schema == collection_schema
            rows = list(zip(result[0].to_pylist(), result[1].to_pylist(), strict=True))
            assert rows == sorted(rows)
            assert current in rows
            assert secondary in rows
            assert result[2].null_count == result.num_rows

            statement.set_options(**{"adbc.metadata.collection": "schemas"})
            statement.bind(
                pyarrow.record_batch(
                    [
                        [secondary[0], None, secondary[0], secondary[0]],
                        [None, "datafus%", None, None],
                        [None, current[1], None, current[1]],
                        ["secondary%", None, "secondary%", None],
                    ],
                    schema=filter_schema,
                )
            )
            handle, _ = statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
            assert result.schema == collection_schema
            assert list(
                zip(result[0].to_pylist(), result[1].to_pylist(), strict=True)
            ) == [
                secondary,
                current,
                secondary,
            ]

            statement.set_options(**{"adbc.metadata.collection": "schemas"})
            statement.bind(pyarrow.record_batch([[], [], [], []], schema=filter_schema))
            handle, _ = statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
            assert result.schema == collection_schema
            assert result.num_rows == 0


def test_uri(driver, driver_path) -> None:
    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            uri="datafusion://",
            autocommit=True,
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.adbc_statement.set_sql_query("SELECT 1")
        handle, _ = cursor.adbc_statement.execute_query()
        assert handle is not None
