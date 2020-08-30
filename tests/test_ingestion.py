from pathlib import Path

import pytest

from us_immigration.ingestion import AirportIngestion, I94modeIngestion
from us_immigration import config


@pytest.fixture
def data_dir():
    base_dir = Path(__file__).parent
    return base_dir / 'data'


@pytest.fixture
def default_options():
    options = dict(
        database=config.ATHENA_DATABASE,
        bucket=config.S3_BUCKET,
        prefix=config.S3_PREPROCESSED_PREFIX,
    )
    return options


class TestAirportIngestion:

    def test_ingest_with_valid_parameters(self,
            mocker, data_dir, default_options):

        mock_to_parquet = mocker.patch('awswrangler.s3.to_parquet')

        # given
        table = config.AIRPORT_TABLE
        database = config.ATHENA_DATABASE
        source = (data_dir / f"test_{table}.csv").resolve()
        ingest = AirportIngestion(
            source=source, table=table, **default_options)

        # when
        ingest()

        # then
        expected_cols = [
            'ident',
            'type',
            'name',
            'elevation_ft',
            'continent',
            'iso_country',
            'iso_region',
            'municipality',
            'gps_code',
            'iata_code',
            'local_code',
            'coordinates',
        ]
        df = mock_to_parquet.call_args.kwargs['df']
        assert df.columns.tolist() == expected_cols

        expected_kwargs = dict(
            df=mocker.ANY,
            path=(
                config.S3_BUCKET / config.S3_PREPROCESSED_PREFIX / table
            ).url,
            table=table,
            database=database,
            partition_cols=['iso_country'],
            dataset=True,
            mode='overwrite_partitions')
        mock_to_parquet.assert_called_once_with(**expected_kwargs)


class TestI94ModeIngestion:

    def test_ingest_with_valid_parameters(
            self, mocker, data_dir, default_options):

        mock_to_parquet = mocker.patch('awswrangler.s3.to_parquet')

        # given
        table = config.I94MODE_TABLE
        database = config.ATHENA_DATABASE
        ingest = I94modeIngestion(table=table, **default_options)

        # when
        ingest()

        # then
        expected_cols = ['code', 'name']
        df = mock_to_parquet.call_args.kwargs['df']
        assert df.columns.tolist() == expected_cols

        expected_kwargs = dict(
            df=mocker.ANY,
            path=(
                config.S3_BUCKET / config.S3_PREPROCESSED_PREFIX / table
            ).url,
            table=table,
            database=database,
            partition_cols=None,
            dataset=True,
            mode='overwrite_partitions')
        mock_to_parquet.assert_called_once_with(**expected_kwargs)
