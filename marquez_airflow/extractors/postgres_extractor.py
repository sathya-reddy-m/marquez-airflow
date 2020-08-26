# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from marquez_airflow.models import AirflowOperatorMeta
from marquez_airflow.sql.parser import SqlParser
from marquez_airflow.extractors import BaseExtractor


class PostgresExtractor(BaseExtractor):
    def __init__(self, operator):
        self._operator = operator

    def extract(self):
        sql_meta = SqlParser.parse(self._operator.sql)
        return AirflowOperatorMeta(
            name=f"{self._operator.dag_id}.{self._operator.task_id}",
            inputs=sql_meta.in_tables,
            outputs=sql_meta.out_tables
        )
