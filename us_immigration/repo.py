import awswrangler as wr

from . import base
from .base import Table


class AthenaRepo(base.Repo):

    def __init__(self, table: Table):
        self.table = table
        self.database = table.database

    def execute_sql(self, sql):
        query_id = wr.athena.start_query_execution(
            sql=sql,
            database=self.database,
        )
        result = wr.athena.wait_query(query_id)
        return result

    def build_sql(self, operation):
        # todo: use request object for checks
        sql = getattr(self.table, operation)
        return sql

    def create(self):
        sql = self.build_sql(operation='ddl')
        result = self.execute_sql(sql)
        return result

    def read(self):
        sql = self.build_sql(operation='query')
        result = self.execute_sql(sql)
        return result

    def update(self):
        insert = self.build_sql(operation='insert')
        query = self.build_sql(operation='query')
        sql = insert + query
        result = self.execute_sql(sql)
        return result

    def delete(self):
        sql = self.build_sql(operation='drop')
        result = self.execute_sql(sql)
        return result
