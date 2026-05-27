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

from . import datafusion


def pytest_generate_tests(metafunc) -> None:
    quirks = datafusion.get_quirks(metafunc.config.getoption("vendor_version"))
    return connection_tests.generate_tests([quirks], metafunc)


class TestConnection(connection_tests.TestConnection):
    pass


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
