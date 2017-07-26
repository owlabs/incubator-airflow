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
from functools import wraps

from flask import Response
from flask import make_response
from flask import request

from airflow.contrib.auth.backends.ldap_auth import LdapUser

_log = logging.getLogger(__name__)

client_auth = None


def init_app(app):
    pass


def _forbidden():
    return Response("Forbidden", 403)


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return Response("Unauthorized", 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_authentication(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if auth:
            try:
                LdapUser.try_login(auth.username, auth.password)
                response = function(*args, **kwargs)
                response = make_response(response)
                return response
            except Exception as exception:
                _log.info("API login failure: {}".format(exception))
            return _forbidden()
        return _unauthorized()
    return decorated
