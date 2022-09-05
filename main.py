import pickle
import logging

from time import sleep
from multiprocessing import Process, Queue
from typing import NoReturn

from helpers.config import config_loader
from helpers.log import listener_process, listener_configurer, worker_configurer
from helpers.filewatcher import sources_latest_date_modified, sources_comparison, process_files


def main() -> NoReturn:
    queue = Queue(-1)
    listener = Process(target=listener_process, args=(listener_configurer, queue))
    listener.start()

    worker_configurer(queue)
    logger_main = logging.getLogger(__name__)

    mode, poll_time, sources, source_exec, source_paths = config_loader('./config.ini')

    logger_main.info(f'FileWatcher started, mode: {mode}, polling interval: {poll_time} sec')

    while True:

        if 'watching' not in locals():

            try:
                with open('./logs/dir_modified_dates.pickle', 'rb') as f:
                    last_check_dates = pickle.load(f)
                    f.close()
            except FileNotFoundError:
                logger_main.warning(f'File not found, probably first start, if error will persist check the access')
            except EOFError:
                logger_main.warning(f'File is empty, check the file if error persist')
            finally:

                if 'last_check_dates' not in locals():
                    last_check_dates = sources_latest_date_modified(sources, mode)
                watching = True

        sleep(poll_time)

        current_check_dates = sources_latest_date_modified(sources, mode)

        sources2refresh = sources_comparison(last_check_dates, current_check_dates)

        last_check_dates = current_check_dates

        checked_at = last_check_dates.get('CHECKED_AT')
        logger_main.debug(f'Last check at: {checked_at}')

        if not sources2refresh:
            continue

        process_files(sources2refresh, source_exec, source_paths, worker_configurer, queue)


if __name__ == '__main__':
    main()
