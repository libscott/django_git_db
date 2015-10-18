import collections
import contextlib
import pygit2
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.utils import DatabaseErrorWrapper
from djangit import cursor, introspection, schema, operations
from djangit import gitly as tree


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


class GitValidation(object):
    def check_field(self, field, from_model=None):
        return []


class GitConnection(object):
    def __init__(self, *args):
        self.branch = tree.Branch(*args)
        self.last_tree = self.branch.tree.oid

    def commit(self):
        if self.branch.tree.oid != self.last_tree:
            import traceback
            msg = list(reversed([l.splitlines()[0].split(' in ')[1] for l in traceback.format_stack()]))
            self.branch.commit(','.join(msg))
            self.last_tree = self.branch.tree.oid

    def close(self):
        pass


class MyDatabaseErrorWrapper(DatabaseErrorWrapper):
    def __exit__(self, exc_type, *args):
        if exc_type is not None:
            print exc_type, args
            import six
            return six.reraise(exc_type, *args)


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'git'

    data_types = type("", (), {'__getitem__': lambda _, k: k})()

    Database = type("Database", (), {
        # TODO: Implement own db exception classes.
        '__getattr__': lambda self, _: type(None)
    })()

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        #self.alias = alias
        self.ops = operations.GitDatabaseOperations(self)
        self.validation = GitValidation()
        self.features = GitConnectionFeatures()
        self.introspection = introspection.GitIntrospection(self)
        #self.in_atomic_block = False
        #self.savepoint_ids = []
        #self.closed_in_transaction = False
        self.autocommit = False

    @property
    def creation(self):
        from djangit import creation
        return creation.GitDatabaseCreation(self)

    @property
    def queries_logged(self):
        return False

    def get_connection_params(self):
        repo = pygit2.Repository(self.settings_dict['REPO'])
        return (repo, self.settings_dict['NAME'])

    def get_new_connection(self, params):
        return GitConnection(*params)

    def init_connection_state(self):
        pass

    def create_cursor(self):
        return cursor.GitCursor(self.connection.branch)

    def _rollback(self):
        print "asked to rollback"
        #assert self.connection.branch.tree.oid == self.autocommit_savepoint

    @property
    def wrap_database_errors(self):
        return MyDatabaseErrorWrapper(self)

    def schema_editor(self):
        return schema.GitSchemaEditor(self)

    def savepoint(self):
        return self.connection.branch.tree.oid

    def _set_autocommit(self, val):
        self.autocommit = val
