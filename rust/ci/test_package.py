# Copyright (c) 2026 ADBC Drivers Contributors
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
import pyarrow


def test_package() -> None:
    with adbc_driver_manager.dbapi.connect(
        driver="datafusion", autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query("SELECT 1")
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                reader.read_all()
