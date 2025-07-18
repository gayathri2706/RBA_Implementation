import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Base():
    DEBUG = False
    # Your App secret key
    SECRET_KEY = "\2\1thisismyscretkey\1\2\e\y\y\h"

    API_KEY = "f0e228cf4535857f2482c1c433eb95d69ba664aab8fc010c523fbaf47921779e"

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Flask-WTF flag for CSRF
    CSRF_ENABLED = True


class DevelopmentConfig(Base):
    DEBUG = True
    DEVELOPMENT = True
    DATABASE_URI = "mysql+mysqlconnector://root:root@localhost:3306/sandman_dev"


class StgConfig(Base):
    DEBUG = False
    DATABASE_URI = "mysql+mysqlconnector://root:S%40ndm%40Ndb_2020%26stg@127.0.0.1/sandman_dev"


class ProductionConfig(Base):
    DEBUG = False
    DATABASE_URI = "mysql+mysqlconnector://root:%23%24%40ndm%40Ndb_2020%26@sandman-mysql/sandman_dev"
    DOC_MYSQL = "sandman-mysql"
