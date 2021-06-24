import os

import pytest

MI_LANDING_KEY = "STORAGE_LANDING_CONNECTION_STRING"
MI_PERSISTENT_KEY = "STORAGE_PERSISTENT_CONNECTION_STRING"
EIGHTBYEIGHT_API_USER = "EIGHTBYEIGHT_API_USER"
EIGHTBYEIGHT_API_PASSWORD = "EIGHTBYEIGHT_API_PASSWORD"
BUILD_VERSION = "BUILD_VERSION"
OPTIC_API_KEY = "OPTIC_API_KEY"
OPTIC_API_SECRET = "OPTIC_API_SECRET"


def _default(val, default):
    return default if val is None else val


def load_config(config_keys, optionals=[]):
    # Order of importance
    # 1. cmdline args
    # 2. environment variables

    config = {key: _default(None, os.getenv(config_keys[key])) for key in config_keys.keys()}

    # Ensure all required config is set.
    for config_key, value in config.items():
        if value is None and config_key not in optionals:
            raise ValueError("Required config: {config_key} is not set.".format(config_key=config_key))

    return config


@pytest.fixture(scope="session")
def mi_config():
    return load_config({MI_LANDING_KEY: MI_LANDING_KEY,
                        MI_PERSISTENT_KEY: MI_PERSISTENT_KEY,
                        BUILD_VERSION: BUILD_VERSION,
                        EIGHTBYEIGHT_API_USER: EIGHTBYEIGHT_API_USER,
                        EIGHTBYEIGHT_API_PASSWORD: EIGHTBYEIGHT_API_PASSWORD,
                        OPTIC_API_KEY: OPTIC_API_KEY,
                        OPTIC_API_SECRET: OPTIC_API_SECRET})
