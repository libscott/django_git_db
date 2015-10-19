from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.introspection import \
    BaseDatabaseIntrospection, TableInfo
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.utils import DatabaseErrorWrapper
from django_gitdb import operations

import git_rdbms


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'git'

    data_types = type("", (), {'__getitem__': lambda _, k: k})()

    Database = git_rdbms

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        #self.alias = alias
        self.ops = operations.GitDatabaseOperations(self)
        self.validation = GitValidation()
        self.features = GitConnectionFeatures()
        self.introspection = GitIntrospection(self)
        #self.in_atomic_block = False
        #self.savepoint_ids = []
        #self.closed_in_transaction = False
        self.autocommit = False

    @property
    def creation(self):
        from django_gitdb import creation
        return creation.GitDatabaseCreation(self)

    @property
    def queries_logged(self):
        return False

    def get_connection_params(self):
        return (self.settings_dict['REPO'], self.settings_dict['NAME'])

    def get_new_connection(self, params):
        return git_rdbms.Connection(*params)

    def init_connection_state(self):
        pass

    def create_cursor(self):
        return self.connection.cursor()

    def _rollback(self):
        self.connection.rollback()

    @property
    def wrap_database_errors(self):
        return MyDatabaseErrorWrapper(self)

    def schema_editor(self):
        return GitSchemaEditor(self)

    def savepoint(self):
        return self.connection.savepoint()

    def _set_autocommit(self, val):
        self.autocommit = val


class GitConnectionFeatures(object):
    gis_enabled = False
    truncates_names = False
    can_rollback_ddl = True
    can_clone_databases = False
    autocommits_when_autocommit_is_off = False
    interprets_empty_strings_as_nulls = False
    can_combine_inserts_with_and_without_auto_increment_pk = True
    uses_savepoints = True
    supports_foreign_keys = True
    implied_column_null = True
    related_fields_match_type = True
    requires_literal_defaults = True
    supports_combined_alters = False
    connection_persists_old_columns = True  # Get the reset in case...
    can_defer_constraint_checks = False


class GitSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_fk = BaseDatabaseSchemaEditor.sql_create_fk \
        .replace(' DEFERRABLE INITIALLY DEFERRED', '')  # Don't support this

    def prepare_default(self, val):
        import json
        return json.dumps(val)


class GitValidation(object):
    def check_field(self, field, from_model=None):
        return []


class MyDatabaseErrorWrapper(DatabaseErrorWrapper):
    def __exit__(self, exc_type, *args):
        if exc_type is not None:
            print exc_type, args
            import six
            return six.reraise(exc_type, *args)


class GitIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        cursor.execute('SHOW TABLES')
        return [TableInfo(name, 't') for (name,) in cursor.fetch_iter()]
