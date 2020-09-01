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

from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from marquez_airflow import DAG
from marquez_airflow.extractors import (Extractors, DefaultExtractor, PostgresExtractor)

from marquez_client.models import SourceType


_DEFAULT_ARGS = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

_DAG = dag = DAG(
    'food_delivery_7_days',
    schedule_interval='@weekly',
    default_args=_DEFAULT_ARGS,
    description='Determines weekly food deliveries.'
)


def test_default_extractor():
    task = BashOperator(
        task_id='echo_dag_info',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=_DAG
    )

    default_extractor = Extractors().extractor_for_task(task)
    task_meta = default_extractor.extract(task)

    assert task_meta.name == 'food_delivery_7_days.echo_dag_info'
    assert task_meta.source_name is None
    assert task_meta.source_type is None
    assert task_meta.inputs == []
    assert task_meta.outputs == []


def test_postgres_extractor():
    task = PostgresOperator(
        task_id='select',
        postgres_conn_id='food_delivery_db',
        sql='SELECT * FROM discounts;',
        dag=_DAG
    )

    postgres_extractor = Extractors().extractor_for_task(task)
    task_meta = postgres_extractor.extract(task)

    assert task_meta.name == 'food_delivery_7_days.select'
    assert task_meta.source_name == 'food_delivery_db'
    assert task_meta.source_type == SourceType.POSTGRESQL
    assert task_meta.inputs == ['discounts']
    assert task_meta.outputs == []
    assert 'sql' in task_meta.context
