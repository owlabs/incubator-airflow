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
#

from airflow import models, settings
from airflow.www.views import dagbag
from airflow.utils.state import State
from datetime import datetime

from flask import Blueprint, jsonify

api_experimental = Blueprint('api_experimental', __name__)


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    if dag_id not in dagbag.dags:
        response = jsonify({'error': 'Dag {} not found'.format(dag_id)})
        response.status_code = 404
        return response

    dag = dagbag.dags[dag_id]
    if not dag.has_task(task_id):
        response = (jsonify({'error': 'Task {} not found in dag {}'
                    .format(task_id, dag_id)}))
        response.status_code = 404
        return response

    task = dag.get_task(task_id)
    fields = {k: str(v) for k, v in vars(task).items() if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/taskstate/dag/<string:dag_id>/task/<string:task_id>/executiondate/<string:execution_date>', methods=['GET'])
def task_state(dag_id, task_id, execution_date):
    """Returns a JSON object with a task instance's properties. """
    # Check DAG exists.
    if dag_id not in dagbag.dags:
        response = jsonify({'error': 'Dag {} not found'.format(dag_id)})
        response.status_code = 404
        return response

    # Check task exists.
    dag = dagbag.dags[dag_id]
    if not dag.has_task(task_id):
        response = (jsonify({'error': 'Task {} not found in dag {}'
                    .format(task_id, dag_id)}))
        response.status_code = 404
        return response

    # Get task instance.
    session = settings.Session()
    task_instance = (
        session.query(models.TaskInstance)
        .filter_by(task_id=task_id, execution_date=execution_date)
        .first()
    )

    # Error if task instance not found.
    if not task_instance:
        response = (jsonify({'error': 'Execution Date {} not found for dag {}'
                    .format(execution_date, dag_id)}))
        response.status_code = 404
        return response

    # Send state of task instance.
    fields = {k: str(v) for k, v in vars(task_instance).items() if
              not k.startswith('_')}
    return jsonify(fields)

@api_experimental.route('/createdagrun/dag/<string:dag_id>/', methods=['POST'])
def create_dag_run(dag_id):
    """
    Creates a new DAG Run and returns a JSON object with the DAG Run's
    properties.
    """
    # Check DAG exists.
    if dag_id not in dagbag.dags:
        response = jsonify({'error': 'Dag {} not found'.format(dag_id)})
        response.status_code = 404
        return response

    # Prepare Dag Run properties.
    execution_date = datetime.now()
    run_id = "api__{:%Y-%m-%dT%H:%M:%S}".format(execution_date)

    # Get DAG object and create run.
    dag = dagbag.dags[dag_id]
    new_dag_run = dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        start_date=datetime.now(),
        state=State.RUNNING,
        external_trigger=True
    )

    # Send state of new DAG run.
    fields = {k: str(v) for k, v in vars(new_dag_run).items() if
              not k.startswith('_')}
    return jsonify(fields)



