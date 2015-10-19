from django.db.backends.base.operations import BaseDatabaseOperations

from djangit import compilers


class GitDatabaseOperations(BaseDatabaseOperations):
    def __init__(self, connection):
        self.connection = connection

    def max_name_length(self):
        return 255

    def compiler(self, suggestion):
        return getattr(compilers, suggestion)

    def bulk_batch_size(self, fields, objs):
        return 10

    def quote_name(self, name):
        return '`%s`' % name

    def check_expression_support(self, thingy):
        return True

    def get_db_converters(self, wat):
        print 'get_db_converters', wat
        return []

    def sql_flush(self, style, tables, seqs, allow_cascade):
        if tables != []:
            import pdb; pdb.set_trace()
            1
        return []

    def sequence_reset_sql(self, *args):
        return []

    def autoinc_sql(self, *args):
        return None




