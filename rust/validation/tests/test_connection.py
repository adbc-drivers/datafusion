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


import adbc_drivers_validation.tests.connection as connection_tests
import pytest

from . import datafusion


def pytest_generate_tests(metafunc) -> None:
    quirks = datafusion.get_quirks(metafunc.config.getoption("vendor_version"))
    return connection_tests.generate_tests([quirks], metafunc)


class TestConnection(connection_tests.TestConnection):
    @pytest.mark.xfail(reason="get_info() not implemented")
    def test_get_info(self, driver, conn, record_property) -> None:
        super().test_get_info(driver, conn, record_property)

    @pytest.mark.xfail(
        reason="get_objects() catalog/schema always present in DataFusion"
    )
    def test_get_objects_catalog(self, conn, driver) -> None:
        super().test_get_objects_catalog(conn, driver)

    @pytest.mark.xfail(
        reason="get_objects() catalog/schema always present in DataFusion"
    )
    def test_get_objects_schema(self, conn, driver) -> None:
        super().test_get_objects_schema(conn, driver)

    @pytest.mark.xfail(reason="get_objects() filter parameters not implemented")
    def test_get_objects_table_invalid_catalog(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_invalid_catalog(conn, driver, get_objects_table)

    @pytest.mark.xfail(reason="get_objects() filter parameters not implemented")
    def test_get_objects_table_invalid_schema(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_invalid_schema(conn, driver, get_objects_table)

    @pytest.mark.xfail(reason="get_objects() filter parameters not implemented")
    def test_get_objects_table_invalid_table(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_invalid_table(conn, driver, get_objects_table)

    @pytest.mark.xfail(reason="get_objects() column_name filter not implemented")
    def test_get_objects_column_filter_column_name(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_filter_column_name(
            conn, driver, get_objects_table
        )

    @pytest.mark.xfail(reason="get_objects() ordinal_position not populated")
    def test_get_objects_column_xdbc(self, conn, driver, get_objects_table) -> None:
        super().test_get_objects_column_xdbc(conn, driver, get_objects_table)
