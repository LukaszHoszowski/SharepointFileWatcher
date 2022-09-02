import logging
import logging.handlers
from collections.abc import Callable
from multiprocessing import Queue
from typing import NoReturn


def listener_configurer() -> NoReturn:
    """
        Creates instance of logger for log listener, set the configuration for logger.

            Returns:
                None
    """
    root = logging.getLogger()
    handler = logging.handlers.RotatingFileHandler('./logs/filewatcher_multi.log', 'a', 1_000_000, 3)
    formatter = logging.Formatter('%(asctime)-20s| %(levelname)-8s| %(processName)-12s| %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    root.addHandler(handler)


def listener_process(configurer: Callable, queue: Queue) -> NoReturn:
    """
        Intercepts logs from the queue and send them to handlers.

            Parameters:
                configurer (Logger): instance of listener logger object
                queue (Queue): container for logs from independent processes

            Returns:
                None
    """
    configurer()
    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import sys, traceback
            print('Log listener died', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


def worker_configurer(queue: Queue) -> NoReturn:
    """
        Creates instance of logger for workers, set the configuration for logger, store logs in shared queue.
            Parameters:
                queue (Queue): container for logs from independent processes

            Returns:
                None
    """
    handler_q = logging.handlers.QueueHandler(queue)
    handler_s = logging.StreamHandler()
    
    formatter = logging.Formatter('%(asctime)-20s| %(levelname)-8s| %(processName)-12s| %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    handler_q.setFormatter(formatter)
    handler_s.setFormatter(formatter)
    
    root = logging.getLogger()
    
    if root.hasHandlers():
        root.handlers.clear()

    root.addHandler(handler_q)
    root.addHandler(handler_s)
    root.setLevel(logging.INFO)
