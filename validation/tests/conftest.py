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

import sys
import typing
from pathlib import Path

import adbc_driver_manager
import adbc_driver_manager.dbapi
import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    db_kwargs,
    manual_test,
    pytest_collection_modifyitems,
)

from . import datafusion


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default="53")


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("datafusion")
    return datafusion.get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    return str(
        Path(__file__).parent.parent.parent
        / f"build/libadbc_driver_{driver.name}.{ext}"
    )


@pytest.fixture(scope="session")
def conn_factory(
    driver_path: str,
    db_kwargs: dict[str, typing.Any],  # noqa:F811
) -> typing.Callable[[], adbc_driver_manager.dbapi.Connection]:
    kwargs = db_kwargs.copy()
    kwargs["driver"] = driver_path
    db = adbc_driver_manager.AdbcDatabase(**kwargs)
    shared_db = adbc_driver_manager.dbapi._SharedDatabase(db)

    def _factory() -> adbc_driver_manager.dbapi.Connection:
        adbc_conn = adbc_driver_manager.AdbcConnection(db)
        return adbc_driver_manager.dbapi.Connection(
            shared_db, adbc_conn, autocommit=True
        )

    return _factory


@pytest.fixture(scope="session", autouse=True)
def _setup_resources(
    conn_factory: typing.Callable[[], adbc_driver_manager.dbapi.Connection],
) -> None:
    with conn_factory() as c:
        with c.cursor() as cursor:
            for statement in [
                "CREATE SCHEMA IF NOT EXISTS secondary",
                "CREATE DATABASE IF NOT EXISTS secondary_catalog",
                "CREATE SCHEMA IF NOT EXISTS secondary_catalog.secondary_schema",
            ]:
                cursor.execute(statement)
