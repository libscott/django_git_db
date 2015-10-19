import attr


"""
SQL DDLs
"""


class DDL(object):
    is_ddl = True


@attr.s
class AddConstraint(DDL):
    name = attr.ib()
    constraint = attr.ib()


@attr.s
class UniqueConstraint(DDL):
    cols = attr.ib()


@attr.s
class ForeignKey(DDL):
    name = attr.ib()
    ref_table = attr.ib()
    ref_cols = attr.ib()
    deferrable = attr.ib()
    initially_deferred = attr.ib()


@attr.s
class CreateIndex(DDL):
    name = attr.ib()
    table = attr.ib()
    cols = attr.ib()


@attr.s
class CreateTable(DDL):
    name = attr.ib()
    fields = attr.ib()


@attr.s
class AlterColumn(DDL):
    name = attr.ib()
    alteration = attr.ib()


@attr.s
class SetDefault(DDL):
    value = attr.ib()


@attr.s
class DropDefault(DDL):
    pass
