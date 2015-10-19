from django.db.backends.base.introspection import \
    BaseDatabaseIntrospection, TableInfo


class GitIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        cursor.execute('SHOW TABLES')
        return [TableInfo(name, 't') for (name,) in cursor.fetch_iter()]
