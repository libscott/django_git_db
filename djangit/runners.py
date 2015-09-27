import cPickle
from django.db import utils


class GitQueryRunner(object):
    def __init__(self, query):
        self.query = t = query
        assert t._annotation_select_cache == None
        assert bool(t._annotations) == 0
        assert bool(t._extra) == 0
        assert t._extra_select_cache == None
        assert t.annotation_select_mask in (set([]), None)
        assert bool(t.context) == 0
        #assert t.default_cols == False # TODO: wtf?
        assert t.deferred_loading == (set([]),True)
        assert t.distinct == False
        assert t.distinct_fields == []
        assert t.external_aliases == set([])
        assert t.extra_order_by == ()
        assert t.extra_select_mask in (None, set([]))
        assert t.extra_tables == ()
        assert t.filter_is_sticky == False
        assert t.group_by == None
        #assert t.high_mark == None
        assert t.low_mark == 0
        assert t.max_depth == 5
        assert t.order_by == []
        assert t.select_for_update == False
        assert t.select_for_update_nowait == False
        #assert t.select_related == False
        assert t.standard_ordering == True
        #assert t.used_aliases == set([])

        if self.query.select:
            cols = self.query.select
        else:
            cols = [f.cached_col for f in self.query.model._meta.fields]
        self.select = [(c, None, None) for c in cols]
        self.klass_info = {
            'select_fields': range(len(self.select)),
            'model': self.query.model
        }
        self.annotation_col_map = {}

    def load_object(self, data):
        return cPickle.loads(data)

    def as_row(self, obj):
        return [obj[c.field.name] for (c,_,_) in self.select]

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
            elif cond.lookup_name == 'gt':
                return obj[cond.lhs.target.name] > cond.rhs
            elif cond.lookup_name == 'gte':
                return obj[cond.lhs.target.name] >= cond.rhs
            elif cond.lookup_name == 'lt':
                return obj[cond.lhs.target.name] < cond.rhs
            elif cond.lookup_name == 'lte':
                return obj[cond.lhs.target.name] <= cond.rhs
            else:
                print "new lookup_name"
                import pdb; pdb.set_trace()
                1


class SelectRunner(GitQueryRunner):
    def run(self, branch):
        key = 'tables/%s/objects' % self.query.model._meta.db_table
        objs = (self.load_object(o) for o in branch[key].values())
        return (self.as_row(o) for o in objs
                               if self.qualify(self.query.where, o))


class InsertRunner(GitQueryRunner):
    def __init__(self, *args):
        super(InsertRunner, self).__init__(*args)
        self.table = self.query.model._meta.db_table

    def run(self, branch):
        insert_ids = []
        table = self.query.model._meta.db_table
        cur_id = len(branch['tables/%s' % table].subtree_or_empty('objects')) + 1
        for obj in self.query.objs:
            pk = cur_id
            cur_id += 1
            record = {f.name: getattr(obj, f.name) for f in self.query.fields}
            record['id'] = pk
            data = cPickle.dumps(record)
            self.update_indexes(branch, record, data)
            key = 'tables/%s/objects/%s' % (table, pk)
            branch[key] = cPickle.dumps(record)
            insert_ids.append(pk)
        return iter(insert_ids)

    def update_indexes(self, branch, record, data):
        path = 'tables/%s/indexes' % self.table
        indexes = list(branch[path])
        for index in indexes:
            cols = index.split(',')
            key = map(record.__getitem__, cols)
            path = 'tables/%s/indexes/%s/%s' % (self.table, index, key)
            if path in branch:
                raise utils.IntegrityError("%s: %s" % (index, key))
            else:
                print path
                branch[path] = data


class UpdateRunner(GitQueryRunner):
    def run(self, branch):
        affected_total = 0
        key = 'tables/%s/objects' % self.query.model._meta.db_table
        for pk in branch[key]:
            obj_key = key + '/' + pk
            obj = self.load_object(branch[obj_key])
            import pdb; pdb.set_trace()
            if self.qualify(self.query.where, obj):
                affected = False
                for (field, thingy, val) in self.query.values:
                    assert thingy == None
                    affected = affected or obj[field.name] != val
                    obj[field.name] = val
                # TODO: self.update_indexes(branch, record, data)
                branch[obj_key] = cPickle.dumps(obj)
                affected_total += int(affected)
        return affected_total


class DeleteRunner(GitQueryRunner):
    def run(self, branch):
        key = 'tables/%s/objects' % self.query.model._meta.db_table
        for pk in branch[key]:
            obj_key = key + '/' + pk
            obj = self.load_object(branch[obj_key])
            if self.qualify(self.query.where, obj):
                branch[obj_key] = None

