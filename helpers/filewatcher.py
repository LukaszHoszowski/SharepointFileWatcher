import os
import glob
import pickle
import logging
from collections.abc import Callable
from pathlib import Path

from time import altzone
from datetime import datetime, timedelta
from collections import namedtuple
from typing import NamedTuple, List, Tuple, Dict, NoReturn
from multiprocessing import Process, Queue


def paths_from_file_pattern(local_path: Path, file_pat: str) -> List:
    """
        Returns List of absolute paths with file name based on given path and file pattern.

            Parameters:
                local_path (Path): path object
                file_pat (str): string with '*' (pattern*.ext), where '*' plays a role of placeholder for variable part
                    of file name

            Returns:
                paths (list): list of absolute paths with file names
    """
    paths = os.path.join(local_path, file_pat)
    return glob.glob(paths)


def fname_extractor(paths: List) -> List:
    """
        Returns Dict with local paths and file names for pandas script.

            Parameters:
                paths (list): Collection of file names

            Returns:
                file_names (list): Collection of file names extracted from given path
    """
    return [os.path.basename(x) for x in paths]


def local_files_modify_dates_extractor(single_source: NamedTuple) -> List:
    """
        Returns collection of file name and last modify date for single source based on local path.
        Supports file name patterns.

            Parameters:
                single_source (NamedTuple): Collection of details regarding single source from sources (paths, ctx, file names)

            Returns:
                local_files_mod_dates (list): Collection of file names and last modify dates from local paths
    """
    local_files_mod_dates = []

    for file, file_params in single_source.files.items():

        if '*' in file:
            file_paths = paths_from_file_pattern(file_params.local_path, file)
            local_files = fname_extractor(file_paths)
        else:
            local_files = file

        file_mod_date = namedtuple("file_modify_date", "fname, modify_date", defaults=[None, None])

        for file_obj in os.scandir(file_params.local_path):
            if os.path.isfile(file_obj.path) and (file_obj.name in local_files):
                local_files_mod_dates.append(file_mod_date(file_obj.name,
                                                           datetime.fromtimestamp(file_obj.stat().st_mtime)))

    return local_files_mod_dates


def sp_files_modify_dates_extractor(single_source: NamedTuple) -> List:
    """
        Returns collection of file name and last modify date for single source for sharepoint resources.
        Supports file name patterns.

            Parameters:
                single_source (NamedTuple): Collection of details regarding single source from sources (paths, ctx, file names)

            Returns:
                sp_files_mod_dates (list): Collection of file names and last modify dates from the sharepoint resources
    """
    sp_files_mod_dates = []
    file_mod_date = namedtuple("file_modify_date", "fname, modify_date", defaults=[None, None])

    for file, file_params in single_source.files.items():
        ctx = file_params.ctx
        folder_url = file_params.sp_url

        folder = ctx.web.get_folder_by_server_relative_url(folder_url).get().execute_query()
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        if '*' in file:
            sp_files = [file_mod_date(f.name, datetime.fromisoformat(f.time_last_modified[:-1]) + timedelta(
                seconds=abs(altzone)))
                        for f in files if
                        f.name.startswith(file.split('*', 1)[0]) and f.name.endswith(file.split('*', 1)[1])]
        else:
            sp_files = [file_mod_date(f.name, datetime.fromisoformat(f.time_last_modified[:-1]) + timedelta(
                seconds=abs(altzone)))
                        for f in files if f.name == file]

        sp_files_mod_dates.extend(sp_files)

    return sp_files_mod_dates


def single_source_latest_date_modified(single_source: NamedTuple, mode: str) -> str:
    """
        Returns last modify date for the latest file in collection attached to single source. Depends on selected mode,
        if mode = 'sharepoint', sharepoint resources will be queried, otherwise local paths.

            Parameters:
                single_source (NamedTuple): Collection of details regarding single source from sources (paths, ctx, file names)
                mode (str): 'sharepoint' or 'local'
            Returns:
                latest_date_modified (str): Single value for last modify date
    """
    logger_watcher = logging.getLogger(__name__)

    if mode == 'sharepoint':
        try:
            files_mod_dates = sp_files_modify_dates_extractor(single_source)
        except AttributeError:
            logger_watcher.debug(
                f'CTX not set, provide credentials in env file, modify date taken from local files for {single_source.name}')
            files_mod_dates = local_files_modify_dates_extractor(single_source)
    else:
        files_mod_dates = local_files_modify_dates_extractor(single_source)

    return max([file.modify_date for file in files_mod_dates]).strftime("%m/%d/%Y %H:%M:%S")


def sources_latest_date_modified(sources: Tuple[NamedTuple], mode: str) -> Dict[str, str]:
    """
        Returns dictionary with last modify dates for each resource and saves it to pickle file,
        each time once files are queried adds 'CHECKED_AT'
        entry which indicates when was last check.

            Parameters:
                sources (tuple): Collection of all sources
                mode (str): 'sharepoint' or 'local'
            Returns:
                sources_latest_mod_dates (dict): Collection of latest modify dates for all the sources
    """
    sources_latest_mod_dates = {'CHECKED_AT': datetime.now().strftime("%m/%d/%Y %H:%M:%S")}

    for source in sources:
        sources_latest_mod_dates[source.name] = single_source_latest_date_modified(source, mode)

    logger_watcher = logging.getLogger(__name__)

    try:
        with open('./logs/dir_modified_dates.pickle', 'wb') as f:
            pickle.dump(sources_latest_mod_dates, f, protocol=pickle.HIGHEST_PROTOCOL)
            f.close()
    except FileNotFoundError:
        logger_watcher.warning(f'IO issue, file is busy')

    return sources_latest_mod_dates


def sources_comparison(last_check_dates: Dict[str, str], current_check_dates: Dict[str, str]) -> List[str]:
    """
        Returns collection of resources which have changed since list last check

            Parameters:
                last_check_dates (dict): Collection of modified dates for all resources from previous check
                current_check_dates (dict): Collection of modified dates for all resources from current check
            Returns:
                modified_sources (list): Collection of all resources which have been modified since last check
    """
    modified_sources = [source for source in current_check_dates
                        if current_check_dates.get(source) != last_check_dates.get(source) and source != 'CHECKED_AT']

    return modified_sources


def process_files(modified_sources: List[str], source_exec: Dict[str, Callable], source_paths: Dict[str, Path],
                  configurer: Callable, queue: Queue) -> NoReturn:
    """
        Process runner, dispatches functions from executables attached to modified resources

            Parameters:
                modified_sources (list): Collection of modified sources since last check
                source_exec (dict): helper dictionary with executable kwargs for dispatched Process
                source_paths (dict): helper dictionary with kwargs for executable function
                configurer (object): logger object for worker to capture logs from independent Process
                queue (object): container for logs transportation between Processes
            Returns:
                None
    """
    processes = []

    configurer(queue)

    for source in modified_sources:
        processes.append(Process(target=source_exec.get(source), kwargs=source_paths.get(source)))

    for process in processes:
        process.start()

    for process in processes:
        process.join()
