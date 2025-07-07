import sys
from redis import Redis
from rq import Connection, Worker, Queue
from logger_config import setup_logging

# Setup Shared Logger
logger = setup_logging()

# Make sure the worker can find the adapter.py module
sys.path.append('.')

listen = ['default']
redis_conn = Redis()

if __name__ == '__main__':
    with Connection(redis_conn):
        worker = Worker(map(Queue, listen))
        logger.info("Worker is starting... listening for jobs on 'default' queue.")
        worker.work()