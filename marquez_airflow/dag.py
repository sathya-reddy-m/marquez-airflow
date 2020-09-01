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
import logging
import time

from pendulum import Pendulum

import airflow

from marquez_airflow import log
from marquez_airflow.utils import get_connection_uri
from marquez_airflow.extractors import Extractors

from marquez_client import MarquezClient
from marquez_client.models import (DatasetType, JobType)

_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_EXTRACTORS = Extractors()


class DAG(airflow.models.DAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._marquez_client = MarquezClient()
        self._marquez_namespace = os.getenv('MARQUEZ_NAMESPACE', 'default')
        self._init()

    def _init(self):
        try:
            self._marquez_client.create_namespace(
                namespace_name=self._marquez_namespace,
                owner_name="anonymous")
        except Exception:
            pass

    def create_dagrun(self, *args, **kwargs):
        # ...
        dag_run = super(DAG, self).create_dagrun(*args, **kwargs)

        start = self._now_ms()
        for task_id, task in self.task_dict.items():
            try:
                self._collect_task_meta(task)
                log.info(
                    f"""
                    Successfully collected task metadata
                    """,
                    marquez_namespace=self._marquez_namespace,
                    duration_ms=(self._now_ms() - start)
                )
            except Exception as e:
                # Log error, then ...
                log.error(
                    f"""
                    Failed to collect task metadata: {e}
                    """,
                    marquez_namespace=self._marquez_namespace,
                    duration_ms=(self._now_ms() - start)
                )
                continue

        return dag_run

    def _collect_task_meta(self, task):
        try:
            # ...
            extractor = _EXTRACTORS.extractor_for_task(task)
            task_meta = extractor.extract(task)

            # (1)
            self._collect_source_meta(task_meta)

            # (2)
            inputs = []
            if task_meta.inputs:
                for input in task_meta.inputs:
                    self._collect_dataset_meta(input, task_meta.source_name)
                inputs = list(
                    map(lambda input: {
                        'namespace': self._marquez_namespace,
                        'name': input
                    }, task_meta.inputs)
                )

            # (3)
            outputs = []
            if task_meta.outputs:
                for output in task_meta.outputs:
                    self._collect_dataset_meta(output, task_meta.source_name)
                outputs = list(
                    map(lambda output: {
                        'namespace': self._marquez_namespace,
                        'name': output
                    }, task_meta.outputs)
                )

            # (4)
            self._marquez_client.create_job(
                namespace_name=self._marquez_namespace,
                job_name=task_meta.name,
                job_type=JobType.BATCH,
                input_dataset=inputs,
                output_dataset=outputs,
                location='https://github.com/' + task_meta.name + '/blob/2294bc15eb49071f38425dc927e48655530a2f2e',
                context=task_meta.context,
                description=self.description)

            log.info(
                f"""
                Successfully collected metadata for task: {task_meta.name}
                """,
                marquez_namespace=self._marquez_namespace
            )
        except Exception as e:
            log.error(
                f"""
                Failed to collect task metadata: {e}
                """,
                marquez_namespace=self._marquez_namespace
            )

    def _collect_source_meta(self, task_meta):
        conn_uri = get_connection_uri(task_meta.source_name)
        self._marquez_client.create_source(
            source_name=task_meta.source_name,
            source_type=task_meta.source_type,
            connection_url=conn_uri)

    def _collect_dataset_meta(self, dataset_name, source_name):
        self._marquez_client.create_dataset(
            namespace_name=self._marquez_namespace,
            dataset_type=DatasetType.DB_TABLE,
            dataset_name=dataset_name,
            physical_name=dataset_name,
            source_name=source_name)

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))

    @staticmethod
    def _to_iso_8601(dt):
        if isinstance(dt, Pendulum):
            return dt.format(_NOMINAL_TIME_FORMAT)
        else:
            return dt.strftime(_NOMINAL_TIME_FORMAT)
