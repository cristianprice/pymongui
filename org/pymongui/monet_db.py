import pymonetdb
from org.pymongui.logging_wrapper import logger


def create_db_connection(username="monetdb", password="monetdb", hostname="localhost", database="demo"):
    logger().info('Trying to create connection: %s %s %s %s', username, '******', hostname, database)
    connection = pymonetdb.connect(username, password, hostname, database)
    logger().info('Connection created ...')
    return connection
