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

from contextlib import closing
from typing import Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from marquez_airflow.models import DbTableSchema, DbColumn
from marquez_airflow.utils import get_connection_uri
from marquez_airflow.extractors.sql.experimental import SqlMeta
from marquez_airflow.extractors.sql.experimental.parser import SqlParser
from marquez_airflow.extractors import (
    BaseExtractor,
    StepMetadata,
    Source,
    Dataset
)

_TABLE_SCHEMA = 1
_TABLE_NAME = 2
_COLUMN_NAME = 3
_ORDINAL_POSITION = 4
_DATA_TYPE = 7


class PostgresExtractor(BaseExtractor):
    operator_class = PostgresOperator

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> [StepMetadata]:
        # (1) Parse sql statement to obtain input / output tables.
        sql_meta: SqlMeta = SqlParser.parse(self.operator.sql)

        # (2) Default all inputs / outputs to current connection.
        # NOTE: We'll want to look into adding support for the `database`
        # property that is used to override the one defined in the connection.
        conn_id = self.operator.postgres_conn_id
        source = Source(
            type='POSTGRESQL',
            name=conn_id,
            connection_url=get_connection_uri(conn_id))

        # (3) Map input / output tables to dataset objects with source set
        # as the current connection.
        inputs = [
            Dataset.from_table_schema(
                source, in_table_schema
            ) for in_table_schema in self._get_table_schemas(
                sql_meta.in_tables
            )
        ]
        outputs = [
            Dataset.from_table_schema(
                source, out_table_schema
            ) for out_table_schema in self._get_table_schemas(
                sql_meta.out_tables
            )
        ]

        return [StepMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=inputs,
            outputs=outputs,
            context={
                'sql': self.operator.sql
            }
        )]

    def _get_table_schemas(self, tables: [str]) -> [DbTableSchema]:
        # Avoid querying postgres by returning an empty array
        # if no tables have been provided.
        if not tables:
            return []

        # Keeps tack of the schema by table.
        schemas_by_table = {}

        postgres = PostgresHook(
            postgres_conn_id=self.operator.postgres_conn_id,
            schema=self.operator.database
        )
        with closing(postgres.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                table_names = ",".join(map(lambda table: f"'{table}'", tables))
                cursor.execute(
                    f"""
                    SELECT *
                      FROM information_schema.columns
                     WHERE table_name IN ({table_names});
                    """
                )
                for row in cursor.fetchall():
                    table_schema_name: str = row[_TABLE_SCHEMA]
                    table_name: str = row[_TABLE_NAME]
                    table_key: str = f"{table_schema_name}.{table_name}"
                    table_column: DbColumn = DbColumn(
                        name=row[_COLUMN_NAME],
                        type=row[_DATA_TYPE],
                        ordinal_position=row[_ORDINAL_POSITION]
                    )
                    table_schema: Optional[DbTableSchema] = \
                        schemas_by_table.get(table_key)
                    if table_schema:
                        schemas_by_table[table_key].\
                            columns.append(table_column)
                    else:
                        schemas_by_table[table_key] = DbTableSchema(
                            schema_name=table_schema_name,
                            table_name=table_name,
                            columns=[table_column]
                        )

        return schemas_by_table.values()
