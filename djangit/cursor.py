from djangit.engine import route_query


class GitCursor(object):
    def __init__(self, branch):
        self.branch = branch

    def execute(self, sql, params=None):
        self.result = route_query(self.branch, sql, params)

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
