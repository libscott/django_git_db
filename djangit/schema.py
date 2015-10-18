import json
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from djangit import gitly as tree


class GitSchemaEditor(BaseDatabaseSchemaEditor):
    def prepare_default(self, val):
        return json.dumps(val)
'''
    def __init__(self, cursor, connection):
        self.cursor = cursor
        self.connection = connection
        self.deferred_sql = []
    def create_model(self, model):
        """ Is this even neccesary? """
        key = 'tables/%s/%%s' % model._meta.db_table
        self.cursor.branch[key % 'objects'] = tree.EMPTY
        self.cursor.branch[key % 'indexes'] = tree.EMPTY

    def alter_unique_together(self, content_type, thingies, field_sets):
        assert len(field_sets) == 1
        table = content_type._meta.db_table
        idx_name = ','.join(sorted(field_sets.pop()))
        key = 'tables/%s/indexes/%s' % (table, idx_name)
        self.cursor.branch[key] = tree.EMPTY

    def alter_field(self, from_model, from_field, to_field):
        # TODO: Support null fields. Or not?
        # TODO: Typing?
        pass

    def remove_field(self, from_model, field):
        key = 'tables/%s/objects' % from_model._meta.db_table
        for obj in self.cursor.branch[key]:
            whoadude()
'''

