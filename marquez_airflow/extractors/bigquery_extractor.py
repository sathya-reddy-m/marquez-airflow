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

import logging
import json

from google.cloud import bigquery

from marquez_airflow.utils import to_dataset, get_job_name
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from marquez_airflow.extractors import BaseExtractor, StepMetadata, Source
from marquez_airflow.extractors.sql.experimental.parser import SqlParser

log = logging.getLogger(__name__)


class BigQueryExtractor(BaseExtractor):
    operator_class = BigQueryOperator

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> [StepMetadata]:
        sql_meta = SqlParser.parse(self.operator.sql)
        log.info("bigquery sql parse successful.")

        conn_id = self.operator.bigquery_conn_id
        source = Source(
            # type=SourceType.BIGQUERY,
            type="BIGQUERY",
            name=conn_id,
            connection_url=conn_id)
        inputs = [
            to_dataset(source, table) for table in sql_meta.in_tables
        ]
        outputs = [
            to_dataset(source, table) for table in sql_meta.out_tables
        ]

        return [StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=outputs,
            context={
                'sql': self.operator.sql
            }
        )]

    def extract_on_complete(self) -> [StepMetadata]:
        steps_meta = self.extract()

        client = bigquery.Client()

        job_name = get_job_name(task=self.operator)
        job = client.get_job(job_id=job_name)

        job_details = json.dumps(job._properties)
        log.debug(job_details)

        steps_meta[0].context = {
            'sql': self.operator.sql,
            'job_details': job_details
        }

        return steps_meta