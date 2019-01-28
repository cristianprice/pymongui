from multiprocessing import Pool
from os import cpu_count

from pymongui.logging_wrapper import logger
from pymongui.monet_db import create_db_connection


def task_execution_context(thread_count=cpu_count()):
    logger().info('Creating task execution context with %s threads.', thread_count)
    pool = Pool(processes=thread_count)
    logger().info('Created ...')
    return pool


if __name__ == '__main__':
    ctx = task_execution_context()
    with create_db_connection() as conn:
        print(conn)
