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
import logging
from flask import Blueprint, jsonify

api_experimental = Blueprint('api_experimental', __name__)
_log = logging.getLogger(__name__)

@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """

    _log.info('TaskInfo API called with parameters: dag_id: {}; '
              'task_id: {}'.format(dag_id, task_id))

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = 'Dag {} not found'.format(dag_id)
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 404
        return response

    # Check task exists.
    dag = dagbag.dags[dag_id]
    if not dag.has_task(task_id):
        error_message = 'Task {} not found in dag {}'.format(task_id, dag_id)
        _log.info(error_message)
        response = (jsonify({'error': error_message}))
        response.status_code = 404
        return response

    task = dag.get_task(task_id)
    fields = {k: str(v) for k, v in vars(task).items() if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/taskstate/dag/<string:dag_id>/task/<string:task_id>/executiondate/<string:execution_date>', methods=['GET'])
def task_state(dag_id, task_id, execution_date):
    """
    Returns a JSON object with a task instance's properties. The format for
    the execution date is expected to be "YYYY-mm-DDTHH:MM:SS", for example:
    "2016-11-16T11:34:15". The colons ought to be escaped to %3A, as you would
    expect, within the URL. These are then automatically replaced by Flask
    before being passed into this method.
    """

    _log.info('TaskState API called with parameters: dag_id: {}; '
              'task_id: {}; execution_date: {}'.format(dag_id,
                                                       task_id,
                                                       execution_date))

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = 'Dag {} not found'.format(dag_id)
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 404
        return response

    # Check task exists.
    dag = dagbag.dags[dag_id]
    if not dag.has_task(task_id):
        error_message = 'Task {} not found in dag {}'.format(task_id, dag_id)
        _log.info(error_message)
        response = (jsonify({'error': error_message}))
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
        error_message = 'Execution Date {} not found for dag {}'.format(
            execution_date, dag_id)
        _log.info(error_message)
        response = (jsonify({'error': error_message}))
        response.status_code = 404
        return response

    # Send state of task instance.
    fields = {k: str(v) for k, v in vars(task_instance).items() if
              not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/createdagrun/dag/<string:dag_id>/', methods=['POST', 'GET'])
def create_dag_run(dag_id):
    """
    Creates a new DAG Run and returns a JSON object with the DAG Run's
    properties.
    """

    _log.info('CreateDAGRun API called with parameters: dag_id: {}'.format(
        dag_id))

    # Create execution_date and pass through to more specific method.
    execution_date = datetime.now().replace(microsecond=0)
    return create_dag_run_for_date(dag_id, execution_date.isoformat())


@api_experimental.route('/createdagrun/dag/<string:dag_id>/executiondate/<string:execution_date>', methods=['POST', 'GET'])
def create_dag_run_for_date(dag_id, execution_date):
    """
    Creates a new DAG Run for the specified date and returns a JSON object
    with the DAG Run's properties. The format for the execution date is
    expected to be "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".
    The colons ought to be escaped to %3A, as you would expect, within the URL.
    These are then automatically replaced by Flask before being passed into
    this method.
    """

    _log.info('CreateDAGRun API called with parameters: dag_id: {}; '
              'execution_date: {}'.format(dag_id, execution_date))

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = 'Dag {} not found'.format(dag_id)
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 404
        return response

    # Prepare Dag Run properties.
    try:
        execution_date = datetime.strptime(execution_date, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        error_message = ('Given execution date, {}, could not be identified '
                         'as a date. Example date format: 2015-11-16T14:34:15'
                         .format(execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400
        return response

    run_id = 'api__{:%Y-%m-%dT%H:%M:%S}'.format(execution_date)

    # Get DAG object and create run.
    dag = dagbag.dags[dag_id]
    new_dag_run = dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        start_date=execution_date,
        state=State.RUNNING,
        external_trigger=True
    )

    # Send state of new DAG run.
    fields = {k: str(v) for k, v in vars(new_dag_run).items() if
              not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/writexcom/dag/<string:dag_id>/task/<string:task_id>/executiondate/<string:execution_date>/key/<string:key>/value/<string:value>', methods=['POST', 'GET'])
def write_to_xcom(dag_id, task_id, execution_date, key, value):
    """
    Writes the given key value pair to the xcom table with the properties
    given. This will update the entry if it already exists, otherwise it will
    create a new entry. The format for the execution date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". The colons ought
    to be escaped to %3A, as you would expect, within the URL. These are then
    automatically replaced by Flask before being passed into this method.
    """

    _log.info('WriteXCom API called with parameters: dag_id: {}; task_id: {}; '
              'execution_date: {}; key: {}; value: {}'.format(dag_id,
                                                              task_id,
                                                              execution_date,
                                                              key,
                                                              value))

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = 'Dag {} not found'.format(dag_id)
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 404
        return response

    # Convert execution_date to a datetime object.
    try:
        execution_date = datetime.strptime(execution_date, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        error_message = ('Given execution date, {}, could not be identified '
                         'as a date. Example date format: 2015-11-16T14:34:15'
                         .format(execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400
        return response

    # Set the XCom object. Duplicate objects are handled and overwritten inside
    # this method.
    models.XCom.set(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        key=key,
        value=value)

    # Retrieve XCom object.
    session = settings.Session()
    xcom = (
        session.query(models.XCom)
        .filter_by(dag_id=dag_id,
                   task_id=task_id,
                   execution_date=execution_date,
                   key=key)
        .first()
    )

    # Send new XCom object.
    fields = {k: str(v) for k, v in vars(xcom).items() if
              not k.startswith('_')}
    return jsonify(fields)
