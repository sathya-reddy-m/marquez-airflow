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

import pytest

from marquez_airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from marquez_airflow.extractors import load_extractors

_DEFAULT_ARGS = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@datakin.com']
}

_DAG = dag = DAG(
    'food_delivery_7_days',
    schedule_interval='@weekly',
    default_args=_DEFAULT_ARGS,
    description='Determines weekly food deliveries.'
)


@pytest.fixture
def extractors():
    return load_extractors()


def test_postgres_extractor(extractors):
    task = PostgresOperator(
        task_id='select',
        postgres_conn_id='food_delivery_db',
        sql='SELECT * FROM discounts;',
        dag=_DAG
    )

    extractor = extractors.get(_extractor_key(task))
    task_meta = extractor(task).extract()

    assert task_meta.name == 'food_delivery_7_days.select'
    assert task_meta.inputs == ['discounts']
    assert task_meta.outputs == []


def _extractor_key(task):
    return task.__class__.__name__