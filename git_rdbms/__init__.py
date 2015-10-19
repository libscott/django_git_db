import pygit2
import gitly
from git_rdbms import database


class Connection(object):
    def __init__(self, repo_path, name):
        repo = pygit2.Repository(repo_path)
        branch = gitly.Branch(repo, name)
        self.database = database.Rdbms(branch)

    def commit(self):
        self.database.commit()

    def close(self):
        pass

    def cursor(self):
        return Cursor(self)

    def savepoint(self):
        return self.database.savepoint()

    def rollback(self):
        self.database.rollback()


class Cursor(object):
    def __init__(self, connection):
        self.db = connection.database

    def execute(self, sql, params=None):
        self.result = self.db.run_sql(sql, params)

    def fetch_iter(self):
        return self.result

    @property
    def rowcount(self):
        return self.result

    def fetchone(self):
        try:
            return self.result.next()
        except StopIteration:
            return None

    def close(self):
        pass

    @property
    def insert_id(self):
        return self.fetch_iter().next()



from git_rdbms.exceptions import *
