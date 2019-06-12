import logging
import logging.config

import os
import yaml


def setup_logging(
    default_path='logging.yaml',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """
    Setup logging configuration
    """
    path = os.getenv(env_key, None) or default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def set_project_env_variables(proj_env_variable, project_directory):
    os.environ[proj_env_variable] = project_directory
