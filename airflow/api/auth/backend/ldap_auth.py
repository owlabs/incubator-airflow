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
    """
    Initialisation function. Current empty, but required.
    """
    pass


def _forbidden():
    """
    Indicate that the given authorization is not valid.
    :return: Flask response object detailing that the user is unauthorized.
    """
    return Response("Forbidden", 403)


def _unauthorized():
    """
    Indicate that authorization is required.
    :return: Flask response object detailing that unauthorization has not been provided.
    """
    return Response("Unauthorized", 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_authentication(function):
    """
    Function to perform authorization and return the appropriate response.
    """
    @wraps(function)
    def decorated(*args, **kwargs):
        # Get authorization header object from the current request.
        auth = request.authorization
        # If there is an authorization header attempt to authenticate.
        if auth:
            try:
                # Call to the existing LDAP Auth module to try and log the user in.
                LdapUser.try_login(auth.username, auth.password)
                # Once authenticated, get the response and return it.
                response = function(*args, **kwargs)
                response = make_response(response)
                return response
            except Exception as exception:
                # If the user could not be logged in, then log thusly and continue.
                _log.info("API login failure: {}".format(exception))
            # If we've got here then the user hasn't been able to authenticate, so return the forbidden response.
            return _forbidden()
        # If we've got here there is no authorization header, so return the unauthorized response.
        return _unauthorized()
    return decorated
