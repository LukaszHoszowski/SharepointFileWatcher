import os
import glob
import pickle
import logging

from time import altzone
from datetime import datetime, timedelta
from collections import namedtuple
from typing import NamedTuple, List, Tuple, Dict
from multiprocessing import Process


def paths_from_file_pattern(local_path, input_fname):
    paths = os.path.join(local_path, input_fname)
    return glob.glob(paths)


def fname_extractor(paths):
    return [os.path.basename(x) for x in paths]


def sp_files_modify_dates_extractor(single_source: NamedTuple) -> List:
    
    file_mod_date = namedtuple("file_modify_date", "fname, modify_date", defaults=[None, None])
    
    sp_ctx = single_source.ctx
    
    folder = sp_ctx.web.get_folder_by_server_relative_url(single_source.sp_url).get().execute_query()
    sp_files = folder.files
    sp_ctx.load(sp_files)
    sp_ctx.execute_query()
    
    if len(single_source.input_fname) == 1 & ('*' in single_source.input_fname):
        file_pattern = single_source.input_fname.split('*', 1)
        sp_files = [file for file in sp_files if (file.name.startswith(file_pattern[0])) &
                    (file.name.endswith(file_pattern[1]))]
    else:
        sp_files = [file for file in sp_files if file.name in [single_source.input_fname]]
    
    sp_files_mod_dates = [file_mod_date(file.name, datetime.fromisoformat(file.time_last_modified[:-1]) +
                                        timedelta(seconds=abs(altzone)))
                          for file in sp_files]
    
    return sp_files_mod_dates


def single_source_latest_date_modified(single_source: NamedTuple) -> str:   
    files_mod_dates = local_files_modify_dates_extractor(single_source)
    
    return max([file.modify_date for file in files_mod_dates]).strftime("%m/%d/%Y %H:%M:%S")


def local_files_modify_dates_extractor(single_source: NamedTuple) -> List:
    
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


def single_source_latest_date_modified_sp(single_source: NamedTuple) -> str:
    if single_source.input_fname and len(single_source.input_fname) == 1:
        file_paths = paths_from_file_pattern(single_source.local_path, *single_source.input_fname)
        monitored_files = fname_extractor(file_paths)
    else:
        monitored_files = single_source.input_fname
    
    files_mod_dates = sp_files_modify_dates_extractor(single_source)
    
    return max([x.modify_date for x in files_mod_dates]).strftime("%m/%d/%Y %H:%M:%S")


def sources_latest_date_modified(sources: Tuple) -> Dict:
    sources_latest_mod_dates = {'CHECKED_AT': datetime.now().strftime("%m/%d/%Y %H:%M:%S")}
    
    for source in sources:
        sources_latest_mod_dates[source.name] = single_source_latest_date_modified(source)
    
    logger_watcher = logging.getLogger(__name__)
    
    try:
        with open('./logs/dir_modified_dates.pickle', 'wb') as f:
            pickle.dump(sources_latest_mod_dates, f, protocol=pickle.HIGHEST_PROTOCOL)
            f.close()
    except FileNotFoundError:
        logger_watcher.warning(f'IO issue, file is busy')
        
    return sources_latest_mod_dates


def sources_comparison(last_check_dates: Dict, current_check_dates: Dict) -> List:
    modified_sources = [source for source in current_check_dates 
                        if current_check_dates.get(source) != last_check_dates.get(source) and source != 'CHECKED_AT']
    
    return modified_sources


def process_files(sources2refresh: List, source_exec, source_paths, configurer, queue) -> None:
    processes = []
        
    configurer(queue)
    
    for source in sources2refresh:
        processes.append(Process(target=source_exec.get(source), kwargs=source_paths.get(source))),

    for process in processes:
        process.start()

    for process in processes:
        process.join()
