import os
import logging
from pathlib import Path, WindowsPath
from dotenv import load_dotenv
from configparser import ConfigParser, ExtendedInterpolation, NoSectionError
from collections import namedtuple
from typing import NamedTuple, Dict, Optional, Any

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext

from executables import *


def source_paths_kwargs(sources: NamedTuple) -> Dict[Dict, Dict]:
    return {source.name: {'source': tuple(source.files.values())[0].local_path,
                          'file': tuple(source.files)[0]} for source in sources}


def source_process_exec(sources: NamedTuple) -> Dict:
    return {source.name: source.func for source in sources}


def sp_connect_client(site_url: str, client_id: str, client_secret: str, mode: str) -> Optional[Any]:
    # example => site_url = "https://corp.sharepoint.com/teams/Sales_Team/"

    logger = logging.getLogger(__name__)

    if mode in ['sharepoint', 'all']:

        try:
            ctx = ClientContext(site_url).with_credentials(ClientCredential(client_id, client_secret))
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            logger.info(f'Connection to {site_url} successful')
            return ctx
        except ValueError:
            logger.warning(f'Connection to {site_url} UNSUCCESSFUL, Client_id, Client_secret not provided or incorrect')
            return None
    else:
        return None


def ctx_parser(config: Any, mode: str) -> Dict:
    ctx_params = {}
    section = 'Sp_ctx'

    for option in config.options(section):
        ctx_params[option] = config.get(section, option, raw=True).strip('\n').split('\n')
        client_id_ref = (option + '_client_id')
        client_secret_ref = (option + '_client_secret')
        client_id = os.getenv(client_id_ref.upper(), default=None)
        client_secret = os.getenv(client_secret_ref.upper(), default=None)
        ctx_params[option][ctx_params[option].index(client_id_ref)] = client_id
        ctx_params[option][ctx_params[option].index(client_secret_ref)] = client_secret

    return {k: sp_connect_client(*v, mode) for (k, v) in ctx_params.items()}


def files_parser(config, ctx_objects) -> Dict:
    files = {}
    section = 'Files2monitor'
    file = namedtuple("file", "local_path, ctx, sp_url",
                      defaults=[None, None, None])

    for filename in config.options(section):
        files[filename] = config.get(section, filename, raw=False).replace('\n', '', 1).split('\n')
        files[filename][0] = Path(files[filename][0])
        files[filename][1] = ctx_objects.get(files[filename][1])
        files[filename] = file(*files[filename])

    return files


def source_parser(config, files2monitor):
    logger = logging.getLogger(__name__)

    section = 'Sources'

    sources = []

    source = namedtuple("source", "name, func, files", defaults=[None, None, None])

    try:
        for option in config.options(section):
            source_params = config.get(section, option, raw=False).replace('\n', '', 1).split('\n')
            module, func_name = source_params[0].split('.', 1)
            source_params[1] = source_params[1].split(',')

            file_params = {}

            for file in source_params[1]:
                file_params[file] = files2monitor.get(file, None)

            source_params[1] = file_params

            func = getattr(globals()[module], func_name)
            source_params.insert(1, func)
            sources.append(source(*source_params))
    except Exception:
        logger.critical(f"Something is wrong with <config.ini> file, check the schema")
        raise

    return tuple(sources)


def config_loader(config_path):
    logger = logging.getLogger(__name__)

    try:
        config = ConfigParser(interpolation=ExtendedInterpolation(), strict=True)
        config.optionxform = str
        config.read(config_path)

        logger.info(f'Config file from {config_path} successfully loaded')

        poll_time = config.getint('Environs', 'poll_time')
        mode = config.get('Environs', 'mode')
    except NoSectionError:
        logger.critical(f"Config file <{config_path}> NOT LOADED. Check file name <config.ini> or path <./config.ini>")
        raise

    env = './.env'

    try:
        load_dotenv(env)
        logger.info(f'Config file from {env} successfully loaded')
    except Exception:
        logger.warning(
            f"Config file <{env}> NOT LOADED. This file is hidden, "
            f"by default should be named <.env> and located in root folder")
        raise

    ctx_objects = ctx_parser(config, mode)
    files2monitor = files_parser(config, ctx_objects)
    sources = source_parser(config, files2monitor)

    source_paths = source_paths_kwargs(sources)
    source_exec = source_process_exec(sources)

    return mode, poll_time, sources, source_exec, source_paths
