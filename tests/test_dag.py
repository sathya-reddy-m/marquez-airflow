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

from datetime import datetime

import pytest

from airflow.utils.state import State
from airflow.operators.postgres_operator import PostgresOperator

from marquez_airflow import DAG

@pytest.mark.skip(reason="no way of currently testing this")
def test_dag():
    dag = DAG(dag_id='food_delivery_7_days', start_date=datetime(2020, 1, 8),)

    PostgresOperator(
        task_id='select',
        postgres_conn_id='food_delivery_db',
        sql='SELECT * FROM discounts;',
        dag=dag
    )

    dag.create_dagrun(run_id='1', state=State.NONE)
