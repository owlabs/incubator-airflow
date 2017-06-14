# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.exceptions import AirflowException
from airflow.models import DagBag
from airflow.api.common.experimental.get_dag import get_dag

_log = logging.getLogger(__name__)


def get_dag_run(dag_id, execution_date):
    """Return the dag object identified by the given dag_id."""
    dagbag = DagBag()

    # Get the DAG run
    dag = get_dag(dag_id)
    dag_run = dag.get_dagrun(execution_date)
    if dag_run is None:
        error_message = ("Dag run with execution date {} for dag id {} " +
                         "not found").format(execution_date, dag_id)
        raise AirflowException(error_message)

    # Return the dag run.
    return dag_run
