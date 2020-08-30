from us_immigration.command import TableCreation
from us_immigration.repo import AthenaRepo
from us_immigration.sql import tables

from awswrangler.exceptions import QueryFailed


import pytest


@pytest.fixture
def repo(mocker):
    mocker.patch('awswrangler.athena.start_query_execution')
    mocker.patch('awswrangler.athena.wait_query')

    database = 'us_immigration'
    repo = AthenaRepo(database, tables)
    return repo


class TestTableCreation:

    def test_call_with_valid_params(self, mocker):
        # given
        table = 'xdim_country'
        repo = mocker.Mock()
        create_table = TableCreation(repo)

        # when
        create_table(table)

        # then
        repo.create.assert_called_once_with(table)

    @pytest.mark.skip
    def test_call_twice(self, repo):
        # given
        table = 'dim_country'
        create_table = TableCreation(repo)

        # when
        with pytest.raises(QueryFailed) as excinfo:
            create_table(table)

        # then
        expected = 'message:Table dim_country already exist'
        assert expected in str(excinfo.value)
