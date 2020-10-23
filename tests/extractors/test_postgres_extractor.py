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

import os
import mock

from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from marquez_airflow import DAG
from marquez_airflow.models import DbTableSchema, DbColumn
from marquez_airflow.extractors import Source, Dataset
from marquez_airflow.extractors.postgres_extractor import PostgresExtractor

from marquez_client.models import DatasetType

CONN_ID = 'food_delivery_db'
CONN_URI = 'postgres://localhost:5432/food_delivery'

DB_SCHEMA_NAME = 'public'
DB_TABLE_NAME = 'discounts'
DB_TABLE_COLUMNS = [
    DbColumn(
        name='id',
        type='INTEGER',
        ordinal_position=1
    ),
    DbColumn(
        name='amount_off',
        type='INTEGER',
        ordinal_position=2
    ),
    DbColumn(
        name='customer_email',
        type='VARCHAR',
        ordinal_position=3
    ),
    DbColumn(
        name='starts_on',
        type='TIMESTAMP',
        ordinal_position=4
    ),
    DbColumn(
        name='ends_on',
        type='TIMESTAMP',
        ordinal_position=5
    )
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME,
    table_name=DB_TABLE_NAME,
    columns=DB_TABLE_COLUMNS
)
NO_DB_TABLE_SCHEMA = []

SQL = f"SELECT * FROM {DB_TABLE_NAME};"

DAG_ID = 'email_discounts'
DAG_OWNER = 'datascience'
DAG_DEFAULT_ARGS = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}
DAG_DESCRIPTION = \
    'Email discounts to customers that have experienced order delays daily'

DAG = dag = DAG(
    DAG_ID,
    schedule_interval='@weekly',
    default_args=DAG_DEFAULT_ARGS,
    description=DAG_DESCRIPTION
)


@mock.patch('marquez_airflow.extractors.postgres_extractor.\
PostgresExtractor._get_table_schemas')
def test_extract(mock_get_table_schemas):
    mock_get_table_schemas.side_effect = \
        [[DB_TABLE_SCHEMA], NO_DB_TABLE_SCHEMA]

    expected_inputs = [
        Dataset(
            type=DatasetType.DB_TABLE,
            name=f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME}",
            source=Source(
                type='POSTGRESQL',
                name=CONN_ID,
                connection_url=CONN_URI
            ),
            fields=[]
        )]

    expected_context = {
        'sql': SQL,
    }

    # Set the environment variable for the connection
    os.environ[f"AIRFLOW_CONN_{CONN_ID.upper()}"] = CONN_URI

    task = PostgresOperator(
        task_id='select',
        postgres_conn_id=CONN_ID,
        sql=SQL,
        dag=DAG
    )

    # NOTE: When extracting operator metadata, only a single StepMetadata
    # object is returned. We'll want to cleanup the Extractor interface to
    # not return an array.
    step_metadata = PostgresExtractor(task).extract()[0]

    assert step_metadata.name == 'email_discounts.select'
    assert step_metadata.inputs == expected_inputs
    assert step_metadata.outputs == []
    assert step_metadata.context == expected_context
