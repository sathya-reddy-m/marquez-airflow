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

import inspect
import pkgutil
import sys

from marquez_airflow.models import AirflowOperatorMeta


def load_extractors():
    """
    This method.
    """

    extractors = {}

    modules = pkgutil.walk_packages(path=__path__, onerror=lambda x: None, prefix=__name__+'.')
    for module in modules:
        try:
            pkgutil.get_loader(module.name).load_module()
            for name, cls in inspect.getmembers(sys.modules[module.name], inspect.isclass):
                if issubclass(cls, BaseExtractor) and cls != BaseExtractor:
                    extractors[_extractor_key(cls)] = cls
        except Exception:
            pass
    return extractors


def _extractor_key(cls):
    return cls.__name__.replace('Extractor', 'Operator')


class BaseExtractor:
    """
    This class ..
    """

    def extract(self):
        """
        This method will extract metadata from an operator.
        """
        raise NotImplementedError()


class DefaultExtractor(BaseExtractor):
    def __init__(self, operator):
        self._operator = operator

    def extract(self):
        return AirflowOperatorMeta(
            name=f"{self._operator.dag_id}.{self._operator.task_id}",
            inputs=[],
            outputs=[]
        )
