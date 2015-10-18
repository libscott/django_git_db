from djangit.engine import git_run_sql


class GitCursor(object):
    def __init__(self, branch):
        self.branch = branch

    def execute(self, f, params=None):
        if callable(f):
            self.result = f(self.branch)
        else:
            self.result = git_run_sql(self.branch, f, params)

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
