# -*- coding: UTF-8 -*-
"""
API Key Check
===============
This module exposes a python decorator for performing check on the API key which needs to be passed for requests made
with secure end-points
"""
from functools import wraps

from flask import current_app as app
from flask import request
from werkzeug.exceptions import Unauthorized

__author__ = "ganeshsankaran@gyandata.com"


def key_auth(route_func):
    """
    Decorator which checks if the API key has been provided in the request. Developers are expected to use
    this decorator on the routes i.e. the controller end-points after the route decorator. For example

    .. code-block:: python

        @blueprint_name.route(<some-url>)
        @key_auth
        def my_route_function():
            do something...

    Internally the function just checks if the request has the API-key as an argument and if so checks it against the
    application's API key. If the check passes the route function gets executed as usual. If it fails a
    :class:`werkzeug.exceptions.Unauthorized` exception is raised

    :param route_func: The view function i.e. the controller end-point/route
    :type route_func: :class:`object`

    :return: The internal auth-check function as an object
    :rtype: :class:`object`
    """

    @wraps(route_func)
    # the new, post-decoration function. Note *args and **kwargs here.
    def auth_check(*args, **kwargs):
        a = request
        if request.values.get('API_KEY') and request.values.get('API_KEY') == app.config["API_KEY"]:
            return route_func(*args, **kwargs)
        else:
            raise Unauthorized("Unauthorized access! The API key was not found in the request!")

    return auth_check
