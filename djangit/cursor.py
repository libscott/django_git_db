
class GitCursor(object):
    def __init__(self, branch):
        self.branch = branch

    def execute(self, f):
        self.result = f(self.branch)

    def fetch_iter(self):
        return self.result

    def close(self):
        pass

    @property
    def insert_id(self):
        return self.fetch_iter().next()
