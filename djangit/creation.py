import sys

from django.db.backends.base.creation import BaseDatabaseCreation
from django.utils.six.moves import input


class GitDatabaseCreation(BaseDatabaseCreation):
    deserialize_db_from_string = NotImplemented
    _digest = NotImplemented
    get_objects = NotImplemented
    sql_create_model = NotImplemented
    sql_destroy_indexes_for_field = NotImplemented
    sql_destroy_indexes_for_fields = NotImplemented
    sql_destroy_indexes_for_model = NotImplemented
    sql_destroy_model = NotImplemented
    sql_for_inline_foreign_key_references = NotImplemented
    sql_for_pending_references = NotImplemented
    sql_indexes_for_field = NotImplemented
    sql_indexes_for_fields = NotImplemented
    sql_indexes_for_model = NotImplemented
    sql_remove_table_constraints = NotImplemented
    sql_table_creation_suffix = NotImplemented

    def test_db_signature(self):
        settings_dict = self.connection.settings_dict
        return (settings_dict['REPO'], settings_dict['NAME'])

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        test_database_name = self._get_test_db_name()
        refname = 'refs/heads/' + test_database_name
        repo = self.connection.cursor().cursor.branch.repo
        exists = refname in repo.listall_references()
        if exists and not keepdb:
            if not autoclobber:
                confirm = input(
                    "Type 'yes' if you would like to try deleting the test "
                    "database '%s', or 'no' to cancel: " % test_database_name)
            if autoclobber or confirm == 'yes':
                repo.lookup_reference(refname).delete()
            else:
                print "Tests cancelled."
                sys.exit(1)
        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        repo = self.connection.cursor().cursor.branch.repo
        refname = 'refs/heads/' + test_database_name
        if refname in repo.listall_references():
            repo.lookup_reference(refname).delete()

