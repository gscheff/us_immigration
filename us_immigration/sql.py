"""domain objects"""
from .import config
from .dimension import (
    DimCountry,
    DimPort,
    DimState,
)
from .fact import (
    FactImmigration
)


options = dict(
    bucket=config.S3_BUCKET,
    prefix=config.S3_PROCESSED_PREFIX,
    database=config.ATHENA_DATABASE,
)


tables = dict(
    dim_country=DimCountry(table=config.DIM_COUNTRY_TABLE, **options),
    dim_port=DimPort(table=config.DIM_PORT_TABLE, **options),
    dim_state=DimState(table=config.DIM_STATE_TABLE, **options),
)
