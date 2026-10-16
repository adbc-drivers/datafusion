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
    def test_meta_collection_catalogs(self, driver, conn) -> None:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_options(
                **{
                    "adbc.metadata.collection": "catalogs",
                }
            )
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
                assert result.schema == pyarrow.schema(
                    [
                        ("catalog_name", pyarrow.string(), True),
                        ("catalog_remarks", pyarrow.string(), True),
                    ]
                )
                assert result[0].to_pylist() == [
                    driver.features.current_catalog,
                    driver.features.secondary_catalog,
                ]


def test_uri(driver, driver_path) -> None:
    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        uri="datafusion://",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query("SELECT 1")
            handle, _ = cursor.adbc_statement.execute_query()
            assert handle is not None
