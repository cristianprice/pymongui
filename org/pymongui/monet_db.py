import pymonetdb
from org.pymongui.logging_wrapper import logger


def create_db_connection(username="monetdb", password="monetdb", hostname="localhost", database="demo"):
    logger().info('Trying to create connection: %s %s %s %s', username, '******', hostname, database)
    try:
        connection = pymonetdb.connect(username=username, password=password, hostname=hostname, database=database)
    except ConnectionError as ex:
        logger().exception('Failed to connect to db.')
        return None
    logger().info('Connection created ...')
    return connection
