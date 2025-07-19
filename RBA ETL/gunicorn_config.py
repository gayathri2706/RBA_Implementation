"""
Gunicorn Configuration
========================
This module contains the core gunicorn configuration parameters
"""
from multiprocessing import cpu_count

# Adding pylint disables
# Since pylint expects global variables to be in UPPER CASE but these are keywords used by gunicorn and cannot be
# upper cased, there is no other way around it than disabling the refactoring warning

# pylint: disable=C0103

__author__ = "dkarthick@ibytecode.com"

# Log to stdout
accesslog = "-"

# The access log format
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Log to stderr
errorlog = "-"

# Pre-load the application before the workers start. Needed to ensure app initialization/table creation
# happens only once
preload_app = True

# Number of gunicorn workers
workers = cpu_count() * 2 + 1

# Set the worker class to gthread
worker_class = "gevent"

# Set the number of threads
threads = 4

# Set the timeout for requests (purposefully set to a very high value of 1 hour)
timeout = 3600

# Set the python path
pythonpath = "/usr/local/lib/python3"
