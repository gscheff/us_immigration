from .base import BaseQueryExecution


class I94Sample(BaseQueryExecution):

    def __init__(self, percent, database):
        super().__init__(database)
        self.percent = percent

    @property
    def stmt(self):
        return f"""
    SELECT * FROM capstone.immigrant TABLESAMPLE BERNOULLI({self.percent});
        """
