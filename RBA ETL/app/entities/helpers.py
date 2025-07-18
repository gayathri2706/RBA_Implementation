# -*- coding: UTF-8 -*-
"""
ORM Helpers
===================
This module contains various ORM related helper functions
"""
import json
from contextlib import contextmanager

from flask import current_app as app
from flask import g as global_thread_local
from flask import request
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker

from urllib.parse import quote

from ..entities.models import Customer
from ..errors.exceptions import SandmanError

__author__ = "ganeshsankaran@gyandata.com"


def construct_engine(bind_key):
    """
    Create an sqlalchemy engine based on its bind key

    :param bind_key: The name of the bind-key as provided in the Flask configuration for the database
    :type bind_key: str

    :return: An sqlalchemy engine
    :rtype: :class:`sqlalchemy.engine.Engine`
    """
    if not issubclass(type(bind_key), str):
        raise TypeError("The argument bind-key should be a string")

    if not bind_key.strip():
        raise ValueError("Invalid bind key")

    if app.config.get("SQLALCHEMY_BINDS") is None:
        raise ValueError("The configuration parameter SQLALCHEMY_BINDS was missing")

    con_str = app.config.get("SQLALCHEMY_BINDS").get(bind_key)

    if con_str is None:
        raise SandmanError("Connection parameters not found for the bind key : {bind_key}", {"bind_key": bind_key})

    sql_debug_flag = False
    if app.config.get("SQLALCHEMY_ECHO") is not None:
        sql_debug_flag = app.config.get("SQLALCHEMY_ECHO")

    pool_size = 5
    if app.config.get("SQLALCHEMY_POOL_SIZE") is not None:
        pool_size = app.config.get("SQLALCHEMY_POOL_SIZE")

    max_overflow = 5
    if app.config.get("SQLALCHEMY_MAX_OVERFLOW") is not None:
        max_overflow = app.config.get("SQLALCHEMY_MAX_OVERFLOW")

    pool_pre_ping = True
    if app.config.get("SQLALCHEMY_POOL_PRE_PING") is not None:
        pool_pre_ping = app.config.get("SQLALCHEMY_POOL_PRE_PING")

    pool_recycle = 3600
    if app.config.get("SQLALCHEMY_POOL_RECYCLE") is not None:
        pool_recycle = app.config.get("SQLALCHEMY_POOL_RECYCLE")

    engine = create_engine(con_str,
                           pool_size=pool_size,
                           max_overflow=max_overflow,
                           echo=sql_debug_flag,
                           pool_pre_ping=pool_pre_ping,
                           pool_recycle=pool_recycle)

    return engine


def get_engine(bind_key):
    """
    Get the sqlalchemy engine based on its bind-key

    :param bind_key: The name of the bind-key as provided in the Flask configuration for the database
    :type bind_key: str

    :return: An sqlalchemy engine
    :rtype: :class:`sqlalchemy.engine.Engine`

    """
    if not issubclass(type(bind_key), str):
        raise TypeError("The bind-key must be a string")

    if not bind_key.strip():
        raise ValueError("Invalid bind-key!")

    if app.config.get("ENGINES") is None:
        raise ValueError("No sqlalchemy engines were found!")

    # If the engine exists
    if app.config.get("ENGINES").get(bind_key) is not None:
        return app.config.get("ENGINES").get(bind_key)

    raise SandmanError("Engine with name {bind_key} not found!", {"bind_key": bind_key})


def create_connection(bind_key):
    """
    Create an sqlalchemy connection based on the bind key

    :param bind_key: The name of the bind-key as provided in the Flask configuration of the database
    :type bind_key: str

    :return: An sqlalchemy connection object
    :rtype: :class:`sqlalchemy.engine.Connection`
    """
    if not issubclass(type(bind_key), str):
        raise TypeError("The bind-key must be a string")

    if not bind_key.strip():
        raise ValueError("Invalid bind-key!")

    engine = get_engine(bind_key)

    return engine.connect()


@contextmanager
def get_connectable(bind_key):
    """
    Get the sqlalchemy connectable based on its bind-key.

    Common usage would be of the form

     .. code-block:: python

        with get_connectable("DB_BIND_KEY") as connection:

            do something ...

    :param bind_key: The name of the bind-key as provided in the Flask configuration for the database
    :type bind_key: str

    :return: An sqlalchemy connectable object
    :rtype: :class:`sqlalchemy.engine.Engine`

    """
    if not issubclass(type(bind_key), str):
        raise TypeError("The bind-key must be a string")

    if not bind_key.strip():
        raise ValueError("Invalid bind-key!")

    try:

        connectable = get_engine(bind_key)

        yield connectable

    except BaseException as ex:

        if ex.args:
            raise SandmanError(ex.args[0], {})

        raise SandmanError("SQL Error!", {})


def prepare_custom_engine():
    # get customer database
    if request.method == 'POST':
        try:
            customer_id = request.form['customer']
        except KeyError as key_err:
            app.logger.error(str(key_err))
            return
    else:
        customer_id = request.args.get('customer')

    if customer_id is None:
        return

    customer = global_thread_local.SESSION.query(Customer).get(customer_id)
    db_settings = json.loads(customer.db_properties)
    #print('db_settings:', db_settings)

    if db_settings is not None:
        teardown_sqlalchemy()

        db_settings["url"] = db_settings["url"].replace("?useSSL=false", "")
        url_settings = db_settings["url"].split("/")
        db_name = url_settings[len(url_settings) - 1]
        if 'DOC_MYSQL' not in app.config:
            db_host = url_settings[len(url_settings) - 2]
        else:
            db_host = app.config['DOC_MYSQL']

        conn_str = app.config.get("SQLALCHEMY_BINDS").get("TENANT_DB")
        conn_str = conn_str.format(db_settings["username"], quote(db_settings["password"]), db_host, db_name, '{}')
        app.config["SQLALCHEMY_BINDS"]["TENANT_DB"] = conn_str

        engine = construct_engine("TENANT_DB")

        app.config["ENGINES"]["TENANT_DB"] = engine

        prepare_sqlalchemy("TENANT_DB")


def prepare_sqlalchemy(key=None):
    """
    Prepare sqlalchemy for the processing of the request. This is process to the request by binding this function to
    the before-request event-hook of Flask.

    This function performs the following actions

        * Creates connections to the databases and sets them in a global thread-local dictionary named CONNECTIONS
        * Creates sessions and sets them in a global thread-local dictionary named SESSION

    :return: Nothing
    :rtype: None
    """

    is_custom = None
    if key is None:
        key = "APP_DB"
        is_custom = True

    # Create a session
    session_factory = sessionmaker(bind=get_engine(key))

    global_thread_local.SESSION = session_factory()

    if is_custom:
        prepare_custom_engine()


def teardown_sqlalchemy(exception=None):
    """
    Remove the session from the thread-local global variable. This is done by binding this method to the event-hook
    teardown-request so that the scoped session is bound to the lifecycle of the request

    :param exception: The exception that was thrown


    :return: Nothing
    :rtype: None
    """

    try:

        if "SESSION" in global_thread_local:
            # Close the session
            global_thread_local.SESSION.close()

            # Pop it off
            global_thread_local.pop("SESSION")

            # reset tenant connection string
            app.config["SQLALCHEMY_BINDS"]["TENANT_DB"] = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'

    except Exception as ex:

        # Swallow the exception as this method itself is never meant to throw exceptions back
        app.logger.error("Error in database session-teardown", exc_info=True)

        # Raise the appropriate exception
        if ex.args:
            raise SandmanError(ex.args[0], {})

        raise SandmanError("Error in database session-teardown", {})


def get_scoped_session():
    """
    Get the scoped session from the thread-local global variable

    :return: An sqlalchemy session
    :rtype: :class:`sqlalchemy.orm.session.Session`
    """
    if "SESSION" in global_thread_local:
        session = global_thread_local.SESSION
        return session

    raise SandmanError("Session not in thread-local global variable", {})


@contextmanager
def transactional_session():
    """
    Create a transactional session from the sqlalchemy engine based on the bind key. The session auto-commits.

    Common usage would be of the form

     .. code-block:: python

        with transactional_session() as session:

            do something ...

    :return: An sqlalchemy session
    :rtype: :class:`sqlalchemy.orm.session.Session`

    """
    session = None

    try:
        # Yield the session.
        session = get_scoped_session()

        yield session

        app.logger.debug("Committing the session")

        # Commit the session
        session.commit()

    except Exception as ex:

        # Log the exception stack-trace
        app.logger.error("Error in transactional session", exc_info=True)

        app.logger.debug("Rolling back session due to an exception...")

        # Exception occurred. Rollback
        session.rollback()

        # Raise the appropriate exception
        if ex.args:
            raise SandmanError(ex.args[0], {})

        raise SandmanError("SQL Error!", {})


def batched_save_or_update(entities, for_update=False, batch_size=10):
    """
    Batch save or update a homogeneous collection of entities into its underlying database table

    :param entities: The collection of entities
    :type entities: list

    :param for_update: A boolean flag indicating if it's for update
    :type for_update: bool

    :param batch_size: The size of the batch
    :type batch_size: int

    :return: Nothing
    :rtype: None
    """
    if not issubclass(type(entities), list):
        raise TypeError("The argument entities has to be a list!")

    if not issubclass(type(for_update), bool):
        raise TypeError("The argument for_update has to be a boolean")

    if not issubclass(type(batch_size), int):
        raise TypeError("The argument batch_size has to be an integer")

    if batch_size <= 0:
        raise ValueError("Invalid batch-size. It cannot be negative or zero!")

    # Get the length of the entity collection
    coll_length = len(entities)

    # Compute the number of chunks
    num_chunks = (coll_length // batch_size) + 1

    # Initialize start and end index variables
    start_idx = None
    end_idx = None

    # Create a running count
    running_cnt = 0

    with transactional_session() as session:

        for chunk_idx in range(num_chunks):

            start_idx = chunk_idx * batch_size
            end_idx = batch_size * (chunk_idx + 1)

            if end_idx > coll_length:
                end_idx = coll_length

            running_cnt += (end_idx - start_idx)

            if for_update:
                for entity in entities[start_idx:end_idx]:
                    session.merge(entity)

            else:
                session.bulk_save_objects(entities[start_idx:end_idx], return_defaults=True)

            if coll_length > 0:
                app.logger.debug("Flushing session...")
                app.logger.debug("Flushed %d of %d records", running_cnt, coll_length)

            session.flush()


def batched_delete(session, entities, batch_size=100):
    """
    Batch delete a homogeneous collection of entities from its underlying database table

    :param session: The sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param entities: The collection of entities
    :type entities: list

    :param batch_size: The size of the batch
    :type batch_size: int

    :return: Nothing
    :rtype: None

    """
    if not issubclass(type(entities), list):
        raise TypeError("The argument entities has to be a list!")

    if not issubclass(type(batch_size), int):
        raise TypeError("The argument batch_size has to be an integer")

    if batch_size <= 0:
        raise ValueError("Invalid batch-size. It cannot be negative or zero!")

    if not entities:
        return

    # Get the length of the entity collection
    coll_length = len(entities)

    # Compute the number of chunks
    num_chunks = (coll_length // batch_size) + 1

    # Initialize start and end index variables
    start_idx = None
    end_idx = None

    # Create a running count
    running_cnt = 0

    for chunk_idx in range(num_chunks):

        start_idx = chunk_idx * batch_size
        end_idx = batch_size * (chunk_idx + 1)

        if end_idx > coll_length:
            end_idx = coll_length

        running_cnt += (end_idx - start_idx)

        for entity in entities[start_idx:end_idx]:
            session.delete(entity)

        if coll_length > 0:
            app.logger.debug("Flushing session...")
            app.logger.debug("Flushed %d of %d records", running_cnt, coll_length)

        session.flush()
