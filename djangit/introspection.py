from django.db.backends.base.introspection import \
    BaseDatabaseIntrospection, TableInfo


class GitIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        names = cursor.branch.get('tables', {}).keys()
        return [TableInfo(name, 't') for name in names]
