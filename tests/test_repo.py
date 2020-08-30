from furl import furl
import pytest

from us_immigration.repo import AthenaRepo


@pytest.fixture
def table():
    from us_immigration.dimension import DimCountry
    table = 'my_table'
    database = 'my_database'
    bucket = furl('s3://my_bucket')
    prefix = 'my/prefix'
    return DimCountry(table, database, bucket, prefix)


class TestAthenaRepo:

    def test_init(self, table):
        # when
        repo = AthenaRepo(table)

        # then
        assert repo.table == table
        assert isinstance(repo.database, str)

    @pytest.mark.parametrize("operation", ['ctas', 'insert', 'ddl', 'drop'])
    def test_build_sql_with_valid_params(self, table, operation):
        # given
        repo = AthenaRepo(table)

        # when
        sql = repo.build_sql(operation)

        # then
        assert table.table in sql

    def test_create_with_valid_params(self, table, mocker):
        # mock external calls
        mock_start_query_execution = mocker.patch(
            'awswrangler.athena.start_query_execution')
        mocker.patch('awswrangler.athena.wait_query')

        # given
        repo = AthenaRepo(table)

        # when
        _ = repo.create()

        # then
        _, kwargs = mock_start_query_execution.call_args
        assert table.table in kwargs['sql']
        assert 'create' in kwargs['sql'].lower()
        assert kwargs['database'] == table.database
