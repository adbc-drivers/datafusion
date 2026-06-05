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
import pyarrow
import pyarrow.dataset
import pytest

from . import datafusion


def pytest_generate_tests(metafunc) -> None:
    quirks = datafusion.get_quirks(metafunc.config.getoption("vendor_version"))
    return statement_tests.generate_tests([quirks], metafunc)


class TestStatement(statement_tests.TestStatement):
    @pytest.mark.xfail(
        reason="DataFusion lightweight updates require special table settings"
    )
    def test_rows_affected(self, driver, conn) -> None:
        super().test_rows_affected(driver, conn)


def test_cli_local_file(driver, conn, tmpdir) -> None:
    table = pyarrow.table(
        {
            "key": [1, 2, 2, 3, 4, 4, 4],
            "value": [1, 2, 3, 4, 5, 6, 7],
        }
    )
    path = tmpdir / "testdata"
    pyarrow.dataset.write_dataset(
        table, path, format="parquet", partitioning=["key"], partitioning_flavor="hive"
    )
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT key, value FROM '{path}' ORDER BY value ASC")
        assert cursor.fetchall() == [
            ("1", 1),
            ("2", 2),
            ("2", 3),
            ("3", 4),
            ("4", 5),
            ("4", 6),
            ("4", 7),
        ]


def test_cli_external_table(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE EXTERNAL TABLE hits
        STORED AS PARQUET
        LOCATION 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet'
        """)
        cursor.execute("SELECT COUNT(*) FROM hits")
        assert cursor.fetchone()[0] == 1000000


def test_cli_remote_files(driver, conn) -> None:
    # Ensure we support file/object storage tables the same way datafusion-cli does
    for stmt, rowcount in [
        (
            "SELECT COUNT(*) FROM 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet'",
            1000000,
        ),
        (
            "SELECT COUNT(*) FROM 's3://altinity-clickhouse-data/nyc_taxi_rides/data/tripdata_parquet/'",
            1310903963,
        ),
    ]:
        with conn.cursor() as cursor:
            cursor.execute(stmt)
            assert cursor.fetchone()[0] == rowcount


def test_cli_commands(driver, conn) -> None:
    # Apparently these are normally supported by the CLI; test that they work here, too
    for stmt in ["SHOW ALL", "SHOW ALL VERBOSE"]:
        with conn.cursor() as cursor:
            cursor.execute(stmt)
            assert len(cursor.fetchall()) > 0
