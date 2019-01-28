from multiprocessing import Pool
from os import cpu_count

from pymongui.logging_utils import logger
from pymongui.monet_utils import create_db_connection
from pymongui.monet_utils import enum_schemas
from pymongui.monet_utils import enum_tables
from pymongui.monet_utils import enum_columns


def task_execution_context(thread_count=cpu_count()):
    logger().info('Creating task execution context with %s threads.', thread_count)
    pool = Pool(processes=thread_count)
    logger().info('Created ...')
    return pool


if __name__ == '__main__':
    ctx = task_execution_context()
    with create_db_connection(database='events') as conn:

        for s in enum_schemas(conn):
            for t in enum_tables(conn, s):
                for c in enum_columns(conn, t):
                    print(c)
