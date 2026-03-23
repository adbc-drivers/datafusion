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

import adbc_drivers_validation.tests.statement as statement_tests
import pytest

from . import datafusion


def pytest_generate_tests(metafunc) -> None:
    quirks = datafusion.get_quirks(metafunc.config.getoption("vendor_version"))
    return statement_tests.generate_tests([quirks], metafunc)


class TestStatement(statement_tests.TestStatement):
    @pytest.mark.xfail(reason="prepare() not implemented")
    def test_prepare(self, driver, conn) -> None:
        super().test_prepare(driver, conn)

    @pytest.mark.xfail(reason="prepare() not implemented")
    def test_parameter_execute(self, driver, conn) -> None:
        super().test_parameter_execute(driver, conn)

    @pytest.mark.xfail(
        reason="DataFusion lightweight updates require special table settings"
    )
    def test_rows_affected(self, driver, conn) -> None:
        super().test_rows_affected(driver, conn)
