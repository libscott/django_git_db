from django.db import models
from django.db.models.sql.constants import (
CURSOR, GET_ITERATOR_CHUNK_SIZE, MULTI, NO_RESULTS, ORDER_DIR, SINGLE,
)

from djangit import query



class GitQueryCompiler(object):
    def __init__(self, query, connection, using):
        self.query = query
        self.connection = connection
        self.using = using


class SQLCompiler(GitQueryCompiler):
    Query = query.SelectQuery

    def __init__(self, query, connection, using):
        self.query = t = query
        self.connection = connection
        assert using == 'default'
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
        assert t.select_related == False
        assert t.standard_ordering == True
        #assert t.used_aliases == set([])

        self.select = type("Select?", (), {})()
        self.klass_info = None # type("KlassInfo?", (), {})()
        self.annotation_col_map = None # type("AnnotationColMap?", (), {})()

    def has_results(self):
        return True  # Lie

    def execute_sql(self, result_type=MULTI):
        if not result_type:
            result_type = NO_RESULTS

        cursor = self.connection.cursor()
        try:
            cursor.execute(self.Query(self.query).run)
        except Exception:
            cursor.close()
            raise

        if result_type == CURSOR:
            return cursor
        if result_type == SINGLE:
            try:
                val = cursor.fetchone()
                if val:
                    return val[0:self.col_count]
                return val
            finally:
                # done with the cursor
                cursor.close()
        if result_type == NO_RESULTS:
            cursor.close()
            return

        return cursor.fetch_iter()

    results_iter = execute_sql



class GitInsertCompiler(GitQueryCompiler):
    Query = query.InsertQuery

    def execute_sql(self, return_id):
        cursor = self.connection.cursor()
        cursor.execute(self.Query(self.query).run)
        if return_id:
            return cursor.insert_id


SQLInsertCompiler = GitInsertCompiler
