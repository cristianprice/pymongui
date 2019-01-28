import pymonetdb
from pymongui.logging_wrapper import logger
from contextlib import contextmanager


@contextmanager
def create_db_connection(username="monetdb", password="monetdb", hostname="localhost", database="demo"):
    logger().info('Trying to create connection: %s %s %s %s', username, '******', hostname, database)

    connection = pymonetdb.connect(username=username, password=password, hostname=hostname, database=database)
    logger().info('Connection created ...')

    yield connection
    connection.close()
