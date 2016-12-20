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

import airflow.api

from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_task_info import get_task_info
from airflow.api.common.experimental.write_xcom import write_xcom as w_xcom
from airflow.exceptions import AirflowException
from airflow.www.app import csrf

from flask import (
    g, Markup, Blueprint, redirect, jsonify, abort, request, current_app, send_file
)
from datetime import datetime

_log = logging.getLogger(__name__)

requires_authentication = airflow.api.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag with an execution date of now unless
    specified in the data.
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    execution_date = None
    if 'execution_date' in data and data['execution_date'] is not None:
        execution_date = data['execution_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.strptime(execution_date,
                                               '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15'
                .format(execution_date))
            _log.info(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    if getattr(g, 'user', None):
        _log.info("User {} created {}".format(g.user, dr))

    response = jsonify(message="Created {}".format(dr))
    return response


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """
    Returns a JSON with a task's public instance variables. If an exec_date is
    passed in with the request JSON data, details of a task instance will be
    returned instead. The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".
    """

    data = request.get_json(force=True)
    execution_date = None
    if 'exec_date' in data:
        execution_date = data['exec_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.strptime(execution_date,
                                               '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15'
                .format(execution_date))
            _log.info(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    try:
        info = get_task_info(dag_id, task_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>/xcom', methods=['POST'])
@requires_authentication
def write_xcom(dag_id, task_id):
    """
    Writes the given key value pair to the xcom table with the properties
    given. This will update the entry if it already exists, otherwise it will
    create a new entry. The format for the execution date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".
    """
    data = request.get_json(force=True)

    # Get execution_date from JSON data.
    execution_date = None
    if 'exec_date' in data:
        execution_date = data['exec_date']
    else:
        error_message = (
            'Required data "exec_date" was not found in the request JSON.')
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

    # Get key from JSON data.
    key = None
    if 'key' in data:
        key = data['key']
    else:
        error_message = (
            'Required data "key" was not found in the request JSON.')
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

    # Get value from JSON data.
    value = None
    if 'value' in data:
        value = data['value']
    else:
        error_message = (
            'Required data "value" was not found in the request JSON.')
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

    # Convert string datetime into actual datetime
    try:
        execution_date = datetime.strptime(execution_date, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15'
            .format(execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        w_xcom(dag_id,
               task_id,
               execution_date,
               key,
               value)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 404
        return response

    response = jsonify(message="XCom {} has been set to {} for task {} "
                               "in DAG {} with the execution date {}"
                               .format(key,
                                       value,
                                       task_id,
                                       dag_id,
                                       execution_date))

    return response
