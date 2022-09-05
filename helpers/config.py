import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from configparser import ConfigParser, ExtendedInterpolation, NoSectionError
from collections import namedtuple
from typing import NamedTuple, Optional
from collections.abc import Callable

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext

from executables import *


def source_paths_kwargs(sources: tuple[NamedTuple]) -> dict[str, dict[str, Path]]:
    """
        Returns Dict with local paths and file names for pandas script.

            Parameters:
                    sources (dict): Dict with all details from parsed config

            Returns:
                    sources_paths (dict): Dict of source Dicts with paths and file names
    """
    return {source.name: {'source': tuple(source.files.values())[0].local_path,
                          'file': tuple(source.files)[0]} for source in sources}


def source_process_exec(sources: tuple[NamedTuple]) -> dict[str, Callable]:
    """
        Returns Dict with key: resource name, value: function object.

            Parameters:
                sources (dict): Dict with all source details from parsed config

            Returns:
                sources_execs (dict): Dict of function objects
    """
    return {source.name: source.func for source in sources}


def sp_connect_client(site_url: str, client_id: str, client_secret: str, mode: str) -> Optional[ClientContext]:
    """
        Returns sharepoint context (connector) objects or None if mode is not 'sharepoint' or incorrect credential
        have been provided or not provided at all. Client ID and Client Secret can be set in '.env' file.

            Parameters:
                site_url (str): String with sharepoint tenant address -> 'config.ini'
                client_id (str): Client ID (login) for sharepoint authentication -> '.env'
                client_secret (str): Client Secret (password) for sharepoint authentication -> '.env'
                mode (str ['sharepoint' | 'local']): working mode of SharepointFileWatcher
                    'sharepoint' -> sharepoint connections prioritized
                    'local' -> local files prioritized

            Returns:
                ctx (object): Sharepoint connection object (ctx)
    """
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


def ctx_parser(config: ConfigParser, mode: str) -> dict[str, Optional[ClientContext]]:
    """
        Returns Dict with sharepoint context object compiled with sharepoint tenant url and client_id, client_secret
        taken from '.env' file.

            Parameters:
                config (object): config object -> 'config.ini'
                mode (str): working mode of application -> '.env'

            Returns:
                ctx_collection (dict): Dict with sharepoint connection object (ctx) of all sources
    """
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


def files_parser(config: ConfigParser, ctx_collection: dict[str, Optional[ClientContext]]) -> dict[str, NamedTuple]:
    """
        Returns Dict of NamedTuples with sources, defaults to None.

            Parameters:
                config (object): config object -> 'config.ini'
                ctx_collection (dict): collection of sharepoint contexts for given tenants

            Returns:
                files (dict): Dict with file name: file local path, sharepoint context, sharepoint url
    """
    files = {}
    section = 'Files2monitor'
    file = namedtuple("file", "local_path, ctx, sp_url",
                      defaults=[None, None, None])

    for filename in config.options(section):
        files[filename] = config.get(section, filename, raw=False).replace('\n', '', 1).split('\n')
        files[filename][0] = Path(files[filename][0])
        files[filename][1] = ctx_collection.get(files[filename][1])
        files[filename] = file(*files[filename])

    return files


def source_parser(config: ConfigParser, files: dict) -> tuple[NamedTuple, ...]:
    """
        Returns Tuple of NamedTuples with sources, defaults to None.

            Parameters:
                config (object): config object -> 'config.ini'
                files (dict): Dict with file name: file local path, sharepoint context, sharepoint url

            Returns:
                sources (dict): Dict of NamedTuples: name of the resource, related function name from executables, files
                as Dict with file details (file local path, sharepoint context, sharepoint url)
    """
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
                file_params[file] = files.get(file, None)

            source_params[1] = file_params

            func = getattr(globals()[module], func_name)
            source_params.insert(1, func)
            sources.append(source(*source_params))
    except Exception:
        logger.critical(f"Something is wrong with <config.ini> file, check the schema")
        raise

    return tuple(sources)


def config_loader(config_path: str) -> tuple[str, int, tuple[NamedTuple, ...], dict[str, Callable], dict[str, dict[str, Path]]]:
    """
        Main function for config parsing. Returns Tuple of all config values based on 'config.ini' and '.env'.

            Parameters:
                config_path (str): config path -> 'config.ini'

            Returns:
                mode: working mode of application ['sharepoint' | 'local']
                poll_time: file location checking interval expressed in seconds
                sources: Dict containing all details regarding monitored files/locations, including sharepoint contexts
                    and attached executable functions to be triggered
                source_exec: helper Dict with collection of all executable functions
                source_paths: helper Dict with collection of all first monitored files (Process kwargs)
    """
    logger = logging.getLogger(__name__)

    try:
        config = ConfigParser(interpolation=ExtendedInterpolation(), strict=True)
        config.optionxform = str
        config.read(config_path)

        logger.info(f'Config file from {config_path} successfully loaded')

        mode = config.get('Environs', 'mode')
        poll_time = config.getint('Environs', 'poll_time')
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
    files = files_parser(config, ctx_objects)
    sources = source_parser(config, files)

    source_paths = source_paths_kwargs(sources)
    source_exec = source_process_exec(sources)

    return mode, poll_time, sources, source_exec, source_paths
