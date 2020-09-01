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

from airflow.operators.postgres_operator import PostgresOperator

from marquez_airflow.models import AirflowTaskMeta
from marquez_airflow.sql.parser import SqlParser

from marquez_client.models import SourceType


def _task_name(task):
    return f"{task.dag_id}.{task.task_id}"


class Extractors:
    def __init__(self):
        self._extractors = {
            type(PostgresOperator): PostgresExtractor()
        }
        self._default = DefaultExtractor()

    def extractor_for_task(self, task):
        self._extractors.get(type(task), default=self._default)


class BaseExtractor:
    def extract(self, task):
        """
        This method will extract metadata from an Airflow operator.
        """
        raise NotImplementedError()


class DefaultExtractor(BaseExtractor):
    def extract(self, task):
        return AirflowTaskMeta(name=_task_name(task))


class PostgresExtractor(BaseExtractor):
    def extract(self, task):
        sql_meta = SqlParser.parse(task.sql)
        return AirflowTaskMeta(
            name=_task_name(task),
            source_type=SourceType.POSTGRESQL,
            source_name=task.postgres_conn_id,
            inputs=sql_meta.in_tables,
            outputs=sql_meta.out_tables,
            context={
                'sql': task.sql
            }
        )
