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

import json
import os
import time

import airflow

from pendulum import Pendulum

from marquez_client import MarquezClient
from marquez_client.models import JobType

from .extractors import load_extractors
from .utils import get_location
from . import log

_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class DAG(airflow.models.DAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._marquez_client = MarquezClient()
        self._marquez_namespace = os.getenv('MARQUEZ_NAMESPACE', 'default')
        self._marquez_dataset_cache = {}
        self._marquez_source_cache = {}
        self._init()

    def _init(self):
        try:
            self._marquez_client.create_namespace(namespace_name=self._marquez_namespace, owner_name="anonymous")
        except Exception:
            pass

    def create_dagrun(self, *args, **kwargs):
        dagrun = super(DAG, self).create_dagrun(*args, **kwargs)

        create_dag_start_ms = self._now_ms()

        run_args = {
            'external_trigger': kwargs.get('external_trigger', False)
        }

        extractors = {}
        try:
            extractors = load_extractors()
        except Exception as e:
            log.warn(f'Failed retrieve extractors: {e}',
                     airflow_dag_id=self.dag_id,
                     marquez_namespace=self._marquez_namespace)

        try:
            # Collect metadata for each task in the DAG
            for task_id, task in self.task_dict.items():
                t = self._now_ms()
                try:
                    self.collect_task_meta(
                        dagrun.run_id,
                        run_args,
                        task,
                        extractors.get(task.__class__.__name__))
                except Exception as e:
                    log.error(f'Failed to record task: {e}',
                              airflow_dag_id=self.dag_id,
                              task_id=task_id,
                              marquez_namespace=self._marquez_namespace,
                              duration_ms=(self._now_ms() - t))

            log.info('Successfully collected task metadata',
                     airflow_dag_id=self.dag_id,
                     marquez_namespace=self._marquez_namespace,
                     duration_ms=(self._now_ms() - create_dag_start_ms))

        except Exception as e:
            log.error(f'Failed to collected task metadata: {e}',
                      airflow_dag_id=self.dag_id,
                      marquez_namespace=self._marquez_namespace,
                      duration_ms=(self._now_ms() - create_dag_start_ms))

        return dagrun

    def collect_task_meta(self, dag_run_id, run_args, task, extractor):
        if extractor:
            try:
                log.info(f'Using extractor {extractor.__name__}',
                         task_type=task.__class__.__name__,
                         airflow_dag_id=self.dag_id,
                         task_id=task.task_id,
                         airflow_run_id=dag_run_id,
                         marquez_namespace=self._marquez_namespace)

                task_metadata = extractor(task).extract()

                inputs = list(map(lambda input: {
                    'namespace': self._marquez_namespace,
                    'name': input},
                    task_metadata.inputs))

                log.info(f"{inputs}")

                outputs = list(map(lambda output: {
                    'namespace': self._marquez_namespace,
                    'name': output},
                    task_metadata.outputs))

                log.info(f"{outputs}")

                self._marquez_client.create_job(
                    namespace_name=self._marquez_namespace,
                    job_name=task_metadata.name,
                    job_type=JobType.BATCH,
                    input_dataset=inputs,
                    output_dataset=outputs,
                    location='https://github.com/' + task_metadata.name + '/blob/2294bc15eb49071f38425dc927e48655530a2f2e',
                    description=self.description)

                log.info(f'Successfully collected metadata for task: {task_metadata.name}',
                         airflow_dag_id=self.dag_id,
                         marquez_namespace=self._marquez_namespace)
            except Exception as e:
                log.error(f'Failed to extract metadata {e}',
                          airflow_dag_id=self.dag_id,
                          task_id=task.task_id,
                          airflow_run_id=dag_run_id,
                          marquez_namespace=self._marquez_namespace)
        else:
            log.warn('Unable to find an extractor.',
                     task_type=task.__class__.__name__,
                     airflow_dag_id=self.dag_id,
                     task_id=task.task_id,
                     airflow_run_id=dag_run_id,
                     marquez_namespace=self._marquez_namespace)

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))

    @staticmethod
    def _to_iso_8601(dt):
        if isinstance(dt, Pendulum):
            return dt.format(_NOMINAL_TIME_FORMAT)
        else:
            return dt.strftime(_NOMINAL_TIME_FORMAT)