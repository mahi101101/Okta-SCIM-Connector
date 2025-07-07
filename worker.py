import sys
from redis import Redis
from rq import Worker, Queue  # Removed 'Connection'
from logger_config import setup_logging

# Setup Shared Logger
logger = setup_logging()

# Make sure the worker can find the adapter.py module
sys.path.append('.')

listen = ['default']
redis_conn = Redis()

if __name__ == '__main__':
    queues = [Queue(name, connection=redis_conn) for name in listen]
    worker = Worker(queues, connection=redis_conn)
    
    logger.info(f"Worker is starting... listening for jobs on {listen} queue(s).")
    worker.work()