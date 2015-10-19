import json

from djangit.engine import parser


def route_query(branch, sql, params):
    if sql == 'DIRECT':
        return params(branch)
    parsed = parser.parse(sql)
    if parsed.is_ddl:
        assert not params
        return branch.schema.apply_ddl(branch, parsed)
    import pdb; pdb.set_trace()  # Unhandled


