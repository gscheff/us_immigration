from . import config
from .ingestion import (
    AirportIngestion,
    CountryIngestion,
    DemographicsIngestion,
    I94Ingestion,
    I94addrIngestion,
    I94cntyIngestion,
    I94modeIngestion,
    I94prtIngestion,
    I94visaIngestion,
    RegionIngestion,
)
from . import view


__all__ = ['config', 'ingest_data', ' build_data_model']


def ingest_data():
    options = dict(
        database=config.ATHENA_DATABASE,
        bucket=config.S3_BUCKET,
        prefix=config.S3_PREFIX,
        profile=config.AWS_PROFILE,
        region=config.AWS_REGION,
    )

    # I94modeIngestion(table=config.I94MODE_TABLE, **options)()
    # I94cntyIngestion(table=config.I94COUNTRY_TABLE, **options)()
    # I94prtIngestion(table=config.I94PORT_TABLE, **options)()
    I94visaIngestion(table=config.I94VISA_TABLE, **options)()
    # I94addrIngestion(table=config.I94ADDR_TABLE, **options)()

    # AirportIngestion(
    #     source=config.DATA_DIR / 'airports.csv',
    #     table=config.AIRPORT_TABLE,
    #     **options)()

    # CountryIngestion(
    #     source=config.DATA_DIR / 'countries.csv',
    #     table=config.COUNTRY_TABLE,
    #     **options)()

    # RegionIngestion(
    #     source=config.DATA_DIR / 'regions.csv',
    #     table=config.REGION_TABLE,
    #     **options)()

    # DemographicsIngestion(
    #     source=config.DATA_DIR / 'us-cities-demographics.csv',
    #     table=config.DEMOGRAPHICS_TABLE,
    #     **options)()

    # I94Ingestion(
    #     source=config.DATA_DIR / 'i94.parquet',
    #     table=config.I94_TABLE,
    #     **options)()


def build_data_model():

    options = dict(
        database=config.ATHENA_DATABASE,
        profile=config.AWS_PROFILE,
        region=config.AWS_REGION,
    )

    create_fact_demographics = view.FactDemographicsView(**options)
    create_fact_immigration = view.FactImmigrationView(**options)

    create_fact_demographics()
    create_fact_immigration()
