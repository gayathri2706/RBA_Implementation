# -*- coding: UTF-8 -*-
"""
Custom Exception Classes
==========================
This module contains custom exception classes used by the application
"""
from werkzeug.exceptions import InternalServerError

__author__ = "ganeshsankaran@gyandata.com"


class SandmanError(InternalServerError):
    """
    Custom exception class for use anywhere in the application.

    :ivar message: The error message. Can contain named format placeholders
    :vartype message: str

    :ivar param_dict: The dictionary of placeholder names to values as key-value pairs
    :vartype param_dict: dict
    """

    def __init__(self, message, param_dict):
        """
        Initialize a new instance of this error
        """

        formatted_message = None

        # Get a nicely formatted message
        if message:
            formatted_message = "APP_ERROR : "
            formatted_message += message.format(**param_dict)

        if formatted_message:
            super(SandmanError, self).__init__(description=formatted_message, response=None)
        else:
            super(SandmanError, self).__init__()
