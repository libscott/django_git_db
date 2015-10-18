import threading
from contextlib import contextmanager
from djangit.engine.grammar import AddConstraint, UniqueConstraint, \
    ForeignKey, CreateIndex, CreateTable, AlterColumn, SetDefault, DropDefault
import re


parsing = threading.local()


def pure(func):
    def inner(*args, **kwargs):
        out, parsing.string = func(parsing.string, *args, **kwargs)
        return out
    return __import__('functools').wraps(func)(inner)


def alt(func, *args, **kwargs):
    save = parsing.string
    try:
        return func(*args, **kwargs)
    except ValueError:
        parsing.string = save


def many(func):
    out = []
    try:
        while True:
            save = parsing.string
            out.append(func())
    except ValueError:
        parsing.string = save
        return out


@pure
def match(string, pattern):
    match = re.match(pattern, string)
    if match:
        length = len(match.group(0))
        ret = (match.groups() or [match.group(0)])[0]
        return ret, string[length:]
    raise ValueError('No match for "%s" against %s', pattern, string[:10])


def fail():
    import pdb; pdb.set_trace()
    raise ValueError("Could not parse: %s" % parsing.string)


def parse(sql):
    """ Parse sql statement into convenient datastructure """
    print sql
    print
    print
    parsing.string = sql
    out = (parse_create_table()
           or parse_release_savepoint()
           or parse_rollback()
           or parse_alter_table()
           or parse_create_index()
           or fail())
    assert parsing.string == '', parsing.string
    return out


def parse_create_table():
    if not alt(match, 'CREATE TABLE '):
        return
    name = parse_name()
    with parens():
        fields = many(parse_field_spec)
    return CreateTable(name, fields)


def parse_alter_table():
    if not alt(match, 'ALTER TABLE '):
        return
    table = parse_name()
    alteration = (parse_add_constraint()
                  or parse_alter_column()
                  or fail())
    return (table, alteration)


def parse_alter_column():
    if not alt(match, 'ALTER COLUMN '):
        return
    name = parse_name()
    alteration = parse_set_default() or parse_drop_default() or fail()
    return AlterColumn(name, alteration)


def parse_set_default():
    if not alt(match, 'SET DEFAULT '):
        return
    return SetDefault(parse_value())


def parse_drop_default():
    if not alt(match, 'DROP DEFAULT'):
        return
    return DropDefault()


def parse_value():
    return match('".*"')


def parse_add_constraint():
    if not alt(match, "ADD CONSTRAINT "):
        return
    name = parse_name()
    constraint = parse_unique_constraint() or parse_add_foreign_key() or fail()
    return AddConstraint(name, constraint)


def parse_unique_constraint():
    if alt(match, 'UNIQUE '):
        with parens():
            return UniqueConstraint(parse_name_list())


def parse_add_foreign_key():
    if not alt(match, "FOREIGN KEY "):
        return
    with parens():
        name = parse_name()
    match('REFERENCES ')
    table = parse_name()
    with parens():
        cols = parse_name_list()
    return ForeignKey(name, table, cols,
                      bool(match('DEFERRABLE ?|')),
                      bool(match('INITIALLY DEFERRED ?|')))


def parse_create_index():
    if not alt(match, "CREATE INDEX "):
        return
    name = parse_name()
    match("ON ")
    table = parse_name()
    with parens():
        cols = parse_name_list()
    return CreateIndex(name, table, cols)


def parse_name_list():
    """ parse (`col1`, `col2` ...) """
    cols = []
    while True:
        cols.append(parse_name())
        if not alt(match, ', '):
            return cols


@contextmanager
def parens():
    match('\(')
    yield
    match('\) *')


parse_name = lambda: match('`([^`]+)` *')


def parse_field_spec():
    field = parse_name()
    type = match('(\w+) ?')
    null = not match('NOT NULL ?|')
    opts = []
    if match('PRIMARY KEY ?|'):
        opts.append('primary key')
    if match('UNIQUE ?|'):
        opts.append('unique')
    opts.sort()
    match(', |')
    return (field, type, null, opts)


def parse_release_savepoint():
    if not alt(match, "RELEASE SAVEPOINT "):
        return
    return ('RELEASE SAVEPOINT', parse_name())


def parse_rollback():
    if not alt(match, "ROLLBACK TO SAVEPOINT "):
        return
    return ('ROLLBACK', parse_name())
