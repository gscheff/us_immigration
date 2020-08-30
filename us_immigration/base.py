from abc import ABCMeta, abstractproperty, abstractmethod

import awswrangler as wr


class Repo(metaclass=ABCMeta):

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def delete(self):
        pass


class Table(metaclass=ABCMeta):

    table: str
    database: str
    external_location: str

    @abstractproperty
    def ddl(self):
        pass

    @abstractproperty
    def ctas(self):
        pass

    @abstractproperty
    def query(self):
        pass

    @abstractproperty
    def insert(self):
        pass

    @abstractproperty
    def drop(self):
        pass


class BaseQueryExecution:

    def __init__(self, database):
        self.database = database

    def __call__(self):

        query_id = wr.athena.start_query_execution(
            sql=self.stmt,
            database=self.database,
        )
        result = wr.athena.wait_query(query_id)
        return result
