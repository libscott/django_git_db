import copy
import cPickle
from django.db import utils

from django_gitdb.selectors import yield_rows
from git_rdbms import proto

"""
Run Django queries directly
"""


class GitQueryRunner(object):
    def setup_runner(self):
        t = self.query
        assert t._annotation_select_cache == None
        #assert bool(t._annotations) == 0
        assert bool(t._extra) == 0
        assert t._extra_select_cache == None
        assert t.annotation_select_mask in (set([]), None)
        assert bool(t.context) == 0
        #assert t.default_cols == False # TODO: wtf?
        assert t.deferred_loading == (set([]),True)
        #assert t.distinct == False
        #assert t.distinct_fields == []
        assert t.external_aliases == set([])
        assert t.extra_order_by == ()
        assert t.extra_select_mask in (None, set([]))
        assert t.extra_tables == ()
        assert t.filter_is_sticky == False
        assert t.group_by == None
        #assert t.high_mark == None
        assert t.low_mark == 0
        assert t.max_depth == 5
        #assert t.order_by == []
        assert t.select_for_update == False
        assert t.select_for_update_nowait == False
        #assert t.select_related == False
        assert t.standard_ordering == True
        #assert t.used_aliases == set([])

    def load_object(self, data):
        return cPickle.loads(data)

    def as_row(self, obj):
        return [obj[c.field.name] for (c,_,_) in self.select]

    def qualify(self, cond, obj):
        if hasattr(cond, 'children'):
            qualifier = all if cond.connector == 'AND' else any
            result = qualifier(self.qualify(c, obj) for c in cond.children)
            if cond.negated:
                result = not result
            return result
        else:
            try:
                lhs = obj[cond.lhs.alias][cond.lhs.target.attname]
            except KeyError:
                lhs = obj[cond.lhs.target.attname]
            if cond.lookup_name == 'in':
                return lhs in cond.rhs
            elif cond.lookup_name == 'exact':
                return lhs == cond.rhs
            elif cond.lookup_name == 'gt':
                return lhs > cond.rhs
            elif cond.lookup_name == 'gte':
                return lhs >= cond.rhs
            elif cond.lookup_name == 'lt':
                return lhs < cond.rhs
            elif cond.lookup_name == 'lte':
                return lhs <= cond.rhs
            else:
                print "new lookup_name"
                import pdb; pdb.set_trace()
                1


class SelectRunner(GitQueryRunner):
    def run(self, db):
        branch = db.branch
        if self.query.distinct:
            # TODO: this?
            pass
        objs = self.all_the_joins(branch)
        objs = self.apply_ordering(objs)
        return yield_rows(self.select, objs)

    def apply_ordering(self, objs):
        if not self.query.order_by:
            return objs
        objs = list(objs)
        if not objs:
            return []
        for key in nodup(self.query.order_by):
            reverse = False
            if key[0] == '-':
                reverse = True
                key = key[1:]
            if key == 'pk':
                key = self.query.model._meta.pk.name
            # It's weird that we're given the raw column names without aliases
            # So we'll have to find it ourselves.
            for (col,_,_) in self.select:
                if col.target.attname == key:
                    objs.sort(key=lambda o: o[col.alias][key], reverse=reverse)
                    break
            else:
                assert False, ("Couldn't find sort column: %s" % col)
        return iter(objs)

    def all_the_joins(self, branch):
        joins = []
        for table in self.query.tables:
            for alias in self.query.table_map[table]:
                joins.append(self.query.alias_map[alias])
        return self.dive(branch, {}, joins)

    def dive(self, branch, row, joins):
        join = joins.pop(0)
        for row in self.iter_table(branch, row, join):
            if joins:
                for row in self.dive(branch, row, joins[:]):
                    yield row
            else:
                if self.qualify(self.query.where, row):
                    yield row

    def iter_table(self, branch, parent_row, join):
        # if we were going to lookup by index, this is where we'd do it.
        key = 'tables/%s/objects' % join.table_name
        for obj in branch.get(key, {}).values():
            row = self.load_object(obj)
            if join.parent_alias:
                a, b = join.join_cols[0]
                if parent_row[join.parent_alias][a] != row[b]:
                    continue
            r = copy.deepcopy(parent_row)
            r[join.table_alias] = row
            yield r


def nodup(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]


class InsertRunner(GitQueryRunner):
    def run(self, db):
        branch = db.branch
        insert_ids = []
        table = self.query.model._meta.db_table
        cur_id = len(branch.get('tables/%s/objects' % table, {})) + 1
        for obj in self.query.objs:
            pk = cur_id
            cur_id += 1
            record = {f.attname: getattr(obj, f.attname) for f in self.query.fields}
            record['id'] = pk
            db.update_indexes(table, record)
            key = 'tables/%s/objects/%s' % (table, pk)
            branch[key] = cPickle.dumps(record)
            insert_ids.append(pk)
        return iter(insert_ids)

    def update_indexes(self, branch, record, data):
        table = self.query.model._meta.db_table
        path = 'tables/%s/indexes' % table
        indexes = list(branch.get(path, {}))
        for index in indexes:
            cols = index.split(',')
            key = map(record.__getitem__, cols)
            path = 'tables/%s/indexes/%s/%s' % (table, index, key)
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
            if self.qualify(self.query.where, obj):
                for (field, thingy, val) in self.query.values:
                    assert thingy == None
                    obj[field.attname] = val
                # TODO: self.update_indexes(branch, record, data)
                branch[obj_key] = cPickle.dumps(obj)
                affected_total += 1
        return affected_total


class DeleteRunner(GitQueryRunner):
    def run(self, branch):
        key = 'tables/%s/objects' % self.query.model._meta.db_table
        affected = 0
        for pk in branch.get(key, {}):
            obj_key = key + '/' + pk
            obj = self.load_object(branch[obj_key])
            if self.qualify(self.query.where, obj):
                branch[obj_key] = None
                affected += 1
        return affected

