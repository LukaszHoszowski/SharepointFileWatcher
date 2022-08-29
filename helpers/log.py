import time
import logging
import logging.handlers


def listener_configurer():
    root = logging.getLogger()
    handler = logging.handlers.RotatingFileHandler('./logs/filewatcher_multi.log', 'a', 1_000_000, 3)
    formatter = logging.Formatter('%(asctime)-20s| %(levelname)-8s| %(processName)-12s| %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    root.addHandler(handler)


def listener_process(configurer, queue):
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


def worker_configurer(queue):
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


def log_start(func_name, logger):
    return logger.info(f'{func_name} started')


def log_module_start(logger):
    return logger.info(f'Main refresh started')


def log_module_end(logger):
    return logger.info(f'Main refresh finished')


def log_source_files(func_name, file_names, df, logger):
    
    try:
        shape = df.sheets
    except Exception:
        shape = 'sheets'
        
    return logger.debug(f'{func_name} sourced files: {file_names} readed to df -> {shape}')


def log_etl(func_name, logger):
    return logger.debug(f'{func_name} ETL process finished')


def log_duration(t0):
    return round(time.time() - t0, 1)


def log_saved(func_name, df, t0, logger):
    return logger.info(f'{func_name} -> {df.shape} finished in {log_duration(t0)} sec')


def log_file_not_found(logger):
    return logger.exception(f'!FATAL ERROR! file/folder not found, check path configuration')
