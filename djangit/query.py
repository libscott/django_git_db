from django.db import models


class GitQuery(object):
    def __init__(self, query):
        self.query = query


class SelectQuery(GitQuery):
    def run(self, branch):
        key = 'tables/%s/objects' % self.query.model._meta.db_table
        objs = (self.load_object(o) for o in branch[key].values())
        return (o for o in objs if self.qualify(self.query.where, o))

    def load_object(self, data):
        pairs = []
        for line in data.splitlines():
            pairs.append(line.split(':', 1))
        return dict(pairs)

    def qualify(self, cond, obj):
        if hasattr(cond, 'children'):
            qualifier = all if self.query.where.connector == 'AND' else any
            assert not self.query.where.negated
            return qualifier(self.qualify(c, obj) for c in self.query.where.children)
        else:
            if cond.lookup_name == 'in':
                return obj[cond.lhs.target.name] in cond.rhs
            elif cond.lookup_name == 'exact':
                return obj[cond.lhs.target.name] == cond.rhs
            else:
                print "new lookup_name"
                import pdb; pdb.set_trace()
                1


class InsertQuery(GitQuery):
    def __init__(self, *args):
        super(InsertQuery, self).__init__(*args)
        self.insert_ids = []

    def run(self, branch):
        table = self.query.model._meta.db_table
        cur_id = len(branch['tables/%s' % table].subtree_or_empty('objects')) + 1
        for obj in self.query.objs:
            pk = cur_id
            cur_id += 1
            data = ['id:%s' % pk]
            data.extend([serialize(obj, f) for f in self.query.fields])
            data = '\n'.join(data)
            key = 'tables/%s/objects/%s' % (table, pk)
            branch[key] = data
            self.insert_ids.append(pk)
        return iter(self.insert_ids)


SERIALIZERS = {
    models.DateTimeField: lambda d: d.isoformat(),
}


def serialize(obj, field):
    pyval = getattr(obj, field.name)
    serialize = SERIALIZERS.get(type(field), lambda v: v)
    val = serialize(pyval) if pyval != None else None
    return "%s:%s" % (field.name, val)
