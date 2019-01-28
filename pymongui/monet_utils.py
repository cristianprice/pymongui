import pymonetdb
from pymongui.logging_utils import logger
from contextlib import contextmanager
from collections import namedtuple

Schema = namedtuple('Schema', ['id', 'name', 'authorization', 'owner', 'system'])
Table = namedtuple('Table', ['id', 'name', 'schema_id', 'query', 'type', 'system', 'commit_action', 'access', 'temporary'])
Column = namedtuple('Column', ['id', 'name', 'type', 'type_digits', 'type_scale', 'table_id', 'default', 'null', 'number', 'storage'])


@contextmanager
def create_db_connection(username="monetdb", password="monetdb", hostname="localhost", database="demo"):
    logger().info('Trying to create connection: %s %s %s %s', username, '******', hostname, database)

    connection = pymonetdb.connect(username=username, password=password, hostname=hostname, database=database)
    logger().info('Connection created ...')

    yield connection
    connection.close()


def enum_schemas(conn):
    '''
    Gets all schemas in the server.
    :param conn: the db connection.
    :return: an iterator of rows with details.
    '''

    cur = conn.cursor()
    cur.execute('select * from sys.schemas order by name asc')

    for row in cur:
        yield Schema(*row)


def enum_tables(conn, schema_details):
    '''
    Returns tables in a schema.
    :param conn: the db connection.
    :param schema_details: the schema details.
    :return:
    '''

    logger().info('Getting tables in schema: %s', schema_details.name)
    cur = conn.cursor()
    cur.execute('select * from sys.tables where schema_id = %s order by name asc', (schema_details.id,))

    for row in cur:
        yield Table(*row)


def enum_columns(conn, table_details):
    '''

    :param conn: the db connection.
    :param table_id: the table details.
    :return:
    '''

    logger().info('Getting columns in table: %s', table_details.name)
    cur = conn.cursor()
    cur.execute('select * from sys.columns where table_id = %s order by name asc', (table_details.id,))

    for row in cur:
        yield Column(*row)
