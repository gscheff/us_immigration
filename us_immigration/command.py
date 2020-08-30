from .repo import AthenaRepo


class TableCreation:

    def __init__(self, repo: AthenaRepo):
        self.repo = repo

    def __call__(self, table):
        result = self.repo.create(table)
        return result


class TableRead:

    def __init__(self, repo):
        self.repo = repo

    def __call__(self, table):
        result = self.repo.read(table)
        return result


class TableUpdate:

    def __init__(self, repo):
        self.repo = repo

    def __call__(self, table):
        result = self.repo.update(table)
        return result


class TableDelete:

    def __init__(self, repo):
        self.repo = repo

    def __call__(self, table):
        result = self.repo.delete(table)
        return result
