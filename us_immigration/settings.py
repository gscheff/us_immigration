"""
https://github.com/sloria/environs
"""

import pathlib
import sys

from environs import Env
from furl import furl


BASE_DIR = pathlib.Path(__file__).parent
DATA_DIR = BASE_DIR.parent / 'data'
prefix = __package__.upper() + '_'


env = Env()


# Register a new parser method for paths
@env.parser_for('furl')
def furl_parser(value):
    return furl(value)


env.read_env()

# AWS_PROFILE = env('AWS_PROFILE')
# AWS_REGION = env('AWS_REGION', default='eu-central-1')


with env.prefixed(prefix):

    # Override in .env for local development
    LOG_LEVEL = env.log_level('LOG_LEVEL', default='WARNING')

    with env.prefixed('AWS_'):
        # ACCESS_KEY_ID =
        # SECRET_ACCESS_KEY =
        S3_BUCKET = env.furl('S3_BUCKET')
        S3_PREPROCESSED_PREFIX = env(
            'S3_PREPROCESSED_PREFIX', default='capstone/public/preprocessed')
        S3_PROCESSED_PREFIX = env(
            'S3_PROCESSED_PREFIX', default='capstone/public/processed')
        ATHENA_DATABASE = env('ATHENA_DATABASE', default='default')
        with env.prefixed('ATHENA_'):
            AIRPORT_TABLE = env('AIRPORT_TABLE', default='airport')
            COUNTRY_TABLE = env('COUNTRY_TABLE', default='country')
            REGION_TABLE = env('REGION_TABLE', default='region')
            DEMOGRAPHICS_TABLE = env(
                'DEMOGRAPHICS_TABLE', default='us_demographics')
            I94_TABLE = env('I94_TABLE', default='i94')
            I94MODE_TABLE = env('I94MODE_TABLE', default='i94mode')
            I94COUNTRY_TABLE = env('I94COUNTRY_TABLE ', default='i94country')
            I94PORT_TABLE = env('I94PORT_TABLE', default='i94port')
            I94VISA_TABLE = env('I94VISA_TABLE', default='i94visa')
            I94ADDR_TABLE = env('I94ADDR_TABLE', default='i94addr')
            FACT_IMMIGRATION_TABLE = env(
                'FACT_IMMIGRATION_TABLE', default='fact_immigration')
            FACT_DEMOGRAPHICS_TABLE = env(
                'FACT_DEMOGRAPHICS_TABLE', default='fact_demographics')
            DIM_COUNTRY_TABLE = env(
                'DIM_COUNTRY_TABLE', default='dim_country')
            DIM_PORT_TABLE = env(
                'DIM_PORT_TABLE', default='dim_port')
            DIM_STATE_TABLE = env(
                'DIM_STATE_TABLE', default='dim_us_state')

# validates all parsed variables and prevents further parsing
env.seal()

# import all upper case attributes from config module
__all__ = [c for c in dir(sys.modules[__name__]) if c.isupper()]
