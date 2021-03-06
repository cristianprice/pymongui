import logging

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)
'''

logging.basicConfig(filename='pymongui.log', level=logging.DEBUG)
'''


def logger():
    return logging.getLogger(__name__)
