import logging
import os

logger = logging.getLogger(__name__)


def load_config():
    """
    Load configuration from Config/{ENV_TYPE}.cfg based on the ENV_TYPE environment variable.
    Uses the directory of this file so it works when deployed to Lambda (/var/task/Config/...).
    """
    env_type = os.environ.get('ENV_TYPE', 'dev').strip().lower()
    if env_type not in ('dev', 'prod'):
        raise ValueError(f"ENV_TYPE must be 'dev' or 'prod', got: {env_type!r}")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'Config', f'{env_type}.cfg')
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    config = {}
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                config[key.strip()] = value.strip()
    logger.info(f"Loaded config from Config/{env_type}.cfg (ENV_TYPE={env_type})")
    return config


# Load config once at cold start based on ENV_TYPE
CONFIG = load_config()
