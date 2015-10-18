import json

from djangit.engine import statements
from djangit.engine import exceptions as exc


class Schema(dict):
    def __init__(self, *args, **kwargs):
        super(Schema, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def apply_ddl(self, branch, ddl):
        if type(ddl) == statements.CreateTable:
            return self.create_table(branch, ddl)
        import pdb; pdb.set_trace()

    def create_table(self, branch, ddl):
        tables = self.setdefault('tables', {})
        if ddl.name in tables:
            raise exc.ProgrammingError("Table %s already exists", ddl.name)
        pass

    @classmethod
    def loads(cls, data):
        return json.loads(data, object_pairs_hook=cls)

    def dumps(self):
        return json.dumps(indent=4, separators=(',', ': '), sort_keys=True)


True
