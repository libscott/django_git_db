from django.db import models
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.constants import (
CURSOR, GET_ITERATOR_CHUNK_SIZE, MULTI, NO_RESULTS, ORDER_DIR, SINGLE,
)

from djangit import runners


class GitQueryCompiler(SQLCompiler):
    def __init__(self, query, connection, using):
        super(GitQueryCompiler, self).__init__(query, connection, using)
        self.setup_query()
        self.setup_runner()

    def execute_sql(self, result_type=MULTI):
        if not result_type:
            result_type = NO_RESULTS

        self.cursor = cursor = self.connection.cursor()
        try:
            cursor.execute(self.run)
        except:
            cursor.close()
            raise

        if self.connection.autocommit:
            self.connection.commit()

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

    def results_iter(self, results=None):
        """
        Returns an iterator over the results from executing this query.
        """
        if results is None:
            results = self.execute_sql(MULTI)
        return results


class SQLCompiler(GitQueryCompiler, runners.SelectRunner):

    def has_results(self):
        if not hasattr(self, 'cursor'):
            self.execute_sql(None)
        return bool(self.cursor.fetchone())
        return True  # Lie


class GitInsertCompiler(GitQueryCompiler, runners.InsertRunner):

    def execute_sql(self, return_id):
        cursor = self.connection.cursor()
        cursor.execute(self.run)
        if return_id:
            return cursor.insert_id


class GitUpdateCompiler(GitQueryCompiler, runners.UpdateRunner):

    def execute_sql(self, return_type=MULTI):
        out = super(GitUpdateCompiler, self).execute_sql(CURSOR)
        return out.rowcount


class GitDeleteCompiler(GitQueryCompiler, runners.DeleteRunner):
    pass


SQLInsertCompiler = GitInsertCompiler
SQLUpdateCompiler = GitUpdateCompiler
SQLDeleteCompiler = GitDeleteCompiler
