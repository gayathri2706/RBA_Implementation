import logging
import os

from flask import Flask, Blueprint

from .controller.etl_controller import test
#from .controllers.report import algo
from .entities.helpers import construct_engine, prepare_sqlalchemy, teardown_sqlalchemy
from .entities.models import base


def create_app():
    """
     Logging configuration
    """
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
    logging.getLogger().setLevel(logging.DEBUG)

    app = Flask(__name__)
    app.config.from_object(os.environ['APP_SETTINGS'])
    app.config['JSON_SORT_KEYS'] = False

    home = Blueprint('home', __name__)

    # Views
    @home.route('/')
    def index():
        return 'Welcome to rba additive etl module!'

    # Register the blue-prints which expose various API end-points
    app.register_blueprint(home)
    app.register_blueprint(test, url_prefix="/test")
    #app.register_blueprint(algo, url_prefix="/report")

    with app.app_context():
        app.config["SQLALCHEMY_BINDS"] = dict()
        app.config["SQLALCHEMY_BINDS"]["APP_DB"] = app.config['DATABASE_URI']
        app.config["SQLALCHEMY_BINDS"]["TENANT_DB"] = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'

        engines = dict()

        app_engine = construct_engine("APP_DB")
        engines["APP_DB"] = app_engine

        # reflect the tables
        base.prepare(app_engine, reflect=True)

        # Set these in the app
        app.config["ENGINES"] = engines

    # Bind the setup and teardown functions for sqlalchemy
    with app.app_context():
        app.before_request(prepare_sqlalchemy)
        app.teardown_request(teardown_sqlalchemy)

    return app

