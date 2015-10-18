import json

from djangit.engine import models, grammar


def run(branch, stmt, params):
    if params:
        import pdb; pdb.set_trace()
    if type(stmt) == grammar.CreateTable:
        return create_table(branch, stmt)


def create_table(branch, stmt):
    schema = json.loads(branch.get('schema/schema.json', '{}'))
    if stmt.name in schema.get('tables', {}):
        raise Exception("Table exists: %s" % stmt.name)
    schema.setdefault('tables', {})['name'] = stmt.fields
    models.create(branch, stmt.name, stmt.fields)

