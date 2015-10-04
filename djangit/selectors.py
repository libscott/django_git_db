from django.db import models
from django.db.models import expressions


# [(Col(django_migrations, migrations.Migration.app),
#     (u'django_migrations.app', []),
#     None),
#   (Col(django_migrations, migrations.Migration.name),
#      (u'django_migrations.name', []),
#      None)]



def yield_rows(cols, objs):
    selectors = [get_selector(c, a, b) for c, a, b in cols]
    agg = any(c[0].contains_aggregate for c in cols)

    for obj in objs:
        row = []
        for selector in selectors:
            row.append(selector(obj))
        if not agg:
            yield row
    if agg:
        yield row


def get_selector(col, _, __):
    if col.contains_aggregate:
        assert not col.extra['distinct']
        assert len(col.source_expressions) == 1
    return selectors[type(col)](col)


class Count(object):
    def __init__(self, col):
        assert col.source_expressions[0].value == '*'
        self.col = col
        self.tot = 0

    def __call__(self, _):
        self.tot += 1
        return self.tot


class Lookup(object):
    def __init__(self, col):
        self.col = col

    def __call__(self, obj):
        return obj[self.col.alias][self.col.target.attname]


selectors = {
    models.Count: Count,
    expressions.Col: Lookup,
}


