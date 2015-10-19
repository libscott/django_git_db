from contextlib import contextmanager
import json

import gitly
from git_rdbms import exceptions as exc, parser, proto, statements


''' Probably a nicer architecture would be for this to be the connection
    class and for all the heavy lifting to be done by operation classes '''


class Rdbms(gitly.Branch):
    def __init__(self, branch):
        self.branch = branch
        self.last_tree = self.branch.tree

    def commit(self):
        if self.branch.tree != self.last_tree:
            import traceback
            msg = list(reversed([l.splitlines()[0].split(' in ')[1]
                                 for l in traceback.format_stack()]))
            self.branch.commit(','.join(msg))
            self.last_tree = self.branch.tree

    def savepoint(self):
        return self.branch.tree.oid

    def rollback(self, op=None):
        if op and self.branch.tree.oid != op.savepoint:
            import pdb; pdb.set_trace()
        if self.last_tree != self.branch.tree:
            self.branch.tree = self.last_tree

    def run_sql(self, sql, params):
        print sql
        if sql == 'DIRECT':
            return params(self.branch)
        cmd = parser.parse(sql)
        if cmd.is_ddl:
            assert not params
            return self.apply_ddl(cmd)
        if type(cmd) == statements.ShowTables:
            return self.show_tables(params)
        if type(cmd) == statements.ReleaseSavepoint:
            return  # Nothing to do
        if type(cmd) == statements.RollbackToSavepoint:
            return self.rollback_to_savepoint(cmd)
        import pdb; pdb.set_trace()  # Unhandled
        1

    def apply_ddl(self, ddl):
        if type(ddl) == statements.CreateTable:
            return self.ddl_create_table(ddl)
        if type(ddl) == statements.AlterTable:
            return self.ddl_alter_table(ddl)
        if type(ddl) == statements.CreateIndex:
            return self.ddl_create_index(ddl)
        import pdb; pdb.set_trace()
        1

    def ddl_create_table(self, ddl):
        self.branch['tables/%s/objects' % ddl.name] = gitly.EMPTY
        with self.table_schema(ddl.name, False) as table:
            table.update({
                'name': ddl.name,
                'columns': {},
                'indexes': {},
                'foreign_keys': {},
                'columns_order': [],
            })
            for column in ddl.columns:
                table['columns'][column.name] = column
                table['columns_order'].append(column.name)
                if column.unique:
                    idx_name = 'FIELD_UNIQUE_%s' % column.name
                    table['indexes'][idx_name] = {
                        'columns': [column.name],
                        'unique': True,
                    }
                elif column.primary_key:
                    idx_name = 'FIELD_PRIMARY_%s' % column.name
                    table['indexes'][idx_name] = {
                        'columns': [column.name],
                        'unique': True,
                    }
        proto.generate(table)

    def ddl_alter_table(self, ddl):
        with self.table_schema(ddl.name, True) as table:
            if type(ddl.alteration) == statements.AddConstraint:
                constraint = ddl.alteration.constraint
                if type(constraint) == statements.UniqueConstraint:
                    table['indexes'][ddl.alteration.name] = {
                        'columns': constraint.cols,
                        'unique': True,
                    }
                    self.update_index(ddl.name, ddl.alteration.name)
                elif type(constraint) == statements.ForeignKey:
                    table['foreign_keys'][ddl.alteration.name] = {
                        'column': constraint.name,
                        'ref_table': constraint.ref_table,
                        'ref_cols': constraint.ref_cols,
                    }
                    self.update_fk(ddl.name, ddl.alteration.name)
                else:
                    import pdb; pdb.set_trace()
            elif type(ddl.alteration) == statements.AlterColumn:
                if type(ddl.alteration.alteration) == statements.SetDefault:
                    table['columns'][ddl.alteration.name]['default'] = \
                        json.loads(ddl.alteration.alteration.value)
                proto.generate(table)
            else:
                import pdb; pdb.set_trace()
                1

    def ddl_create_index(self, ddl):
        with self.table_schema(ddl.table, True) as table:
            table['indexes'][ddl.name] = {
                'columns': ddl.cols,
            }
        self.update_index(ddl.table, ddl.name)


    def show_tables(self, _):
        return iter(zip(self.branch.get('tables', [])))

    def update_index(self, table, name):
        pass

    def update_fk(self, table, name):
        pass

    @contextmanager
    def table_schema(self, name, should_exist):
        key = 'tables/%s/schema.json' % name
        if should_exist != (key in self.branch):
            desc = ['already exists', 'does not exist'][int(should_exist)]
            raise exc.ProgrammingError("Table %s %s" % (name, desc))
        table = json.loads(self.branch[key]) if should_exist else {}
        yield table
        self.branch[key] = pretty_dumps(table)


def pretty_dumps(data):
    return json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
