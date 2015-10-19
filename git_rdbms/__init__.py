from djangit import gitly
from djangit.engine.schema import Schema
from djangit.engine import parser


class Branch(gitly.Branch):
    @property
    def schema(self):
        return Schema.loads(self.get('schemas/default/schema.json', '{}'))


def route_query(branch, sql, params):
    if sql == 'DIRECT':
        return params(branch)
    cmd = parser.parse(sql)
    import pdb; pdb.set_trace()
    if cmd.is_ddl:
        assert not params
        return branch.schema.apply_ddl(branch, cmd)
    import pdb; pdb.set_trace()  # Unhandled


from djangit.engine.exceptions import *
