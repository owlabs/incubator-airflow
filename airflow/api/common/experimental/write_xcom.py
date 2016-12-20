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

from airflow.exceptions import AirflowException
from airflow.models import DagBag, XCom

import logging

_log = logging.getLogger(__name__)


def write_xcom(dag_id, task_id, execution_date, key, value):
    _log.info('WriteXCom API called with parameters: dag_id: {}; task_id: {}; '
              'execution_date: {}; key: {}; value: {}'.format(dag_id,
                                                              task_id,
                                                              execution_date,
                                                              key,
                                                              value))

    dagbag = DagBag()

    if dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(dag_id)
        raise AirflowException(error_message)

    # Set the XCom object. Duplicate objects are handled and overwritten inside
    # this method.
    XCom.set(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        key=key,
        value=value)

    return
