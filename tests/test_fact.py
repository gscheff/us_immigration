from datetime import date

from furl import furl

from us_immigration import fact
from us_immigration import base


class TestInsertFactImmigrationTable:

    def test_stmt_with_valid_params(self):

        # when
        day = date(2020, 3, 1)
        table = 'my_table'
        database = 'my_database'
        executor = fact.InsertFactImmigrationTable(day, table, database)
        stmt = executor.stmt

        # then
        expected = f"year || month || day = '{day.strftime('%Y%m%d')}'"
        assert expected in stmt

        expected = f"INSERT INTO {database}.{table}"
        assert expected in stmt

    def test_insert_with_valid_params(self, mocker):
        mock_start_query_execution = mocker.patch(
            'awswrangler.athena.start_query_execution')
        mocker.patch('awswrangler.athena.wait_query')

        # when
        day = date(2020, 3, 1)
        table = 'my_table'
        database = 'my_database'
        insert = fact.InsertFactImmigrationTable(day, table, database)
        insert()

        # then
        stmt = mock_start_query_execution.call_args.kwargs['sql']
        expected = "year || month || day = '20200301'"
        assert expected in stmt

        expected = "INSERT INTO my_database.my_table"
        assert expected in stmt

        expected_kwargs = dict(
            sql=mocker.ANY,
            database=database,
        )
        mock_start_query_execution.assert_called_once_with(**expected_kwargs)


class TestUpsertFactImmigrationTable:

    def test_init_with_valid_params(self):

        # when
        day = date(2020, 3, 1)
        table = 'my_table'
        database = 'my_database'
        bucket = furl('s3://my_bucket')
        prefix = 'my/prefix'
        executor = fact.UpsertFactImmigrationTable(
            day=day,
            table=table,
            database=database,
            bucket=bucket,
            prefix=prefix,
        )

        # then
        expected_path = (
            's3://my_bucket/my/prefix/my_table/'
            'year=2020/month=03/day=01'
        )
        assert executor.path == expected_path

        expected_class = base.BaseQueryExecution
        assert isinstance(executor.insert, expected_class)

    def test_delete_with_valid_params(self, mocker):
        mock_delect_objects = mocker.patch('awswrangler.s3.delete_objects')

        # when
        day = date(2020, 3, 1)
        table = 'my_table'
        database = 'my_database'
        bucket = furl('s3://my_bucket')
        prefix = 'my/prefix'
        upsert = fact.UpsertFactImmigrationTable(
            day=day,
            table=table,
            database=database,
            bucket=bucket,
            prefix=prefix,
        )
        upsert.delete()

        # then
        expected_path = (
            's3://my_bucket/my/prefix/my_table/'
            'year=2020/month=03/day=01'
        )
        expected_args = [expected_path]
        mock_delect_objects.assert_called_once_with(*expected_args)
