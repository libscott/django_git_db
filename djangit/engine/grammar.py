import attr


"""
SQL Grammar objects
"""


@attr.s
class AddConstraint(object):
    name = attr.ib()
    constraint = attr.ib()


@attr.s
class UniqueConstraint(object):
    cols = attr.ib()


@attr.s
class ForeignKey(object):
    name = attr.ib()
    ref_table = attr.ib()
    ref_cols = attr.ib()
    deferrable = attr.ib()
    initially_deferred = attr.ib()


@attr.s
class CreateIndex(object):
    name = attr.ib()
    table = attr.ib()
    cols = attr.ib()


@attr.s
class CreateTable(object):
    name = attr.ib()
    fields = attr.ib()


@attr.s
class AlterColumn(object):
    name = attr.ib()
    alteration = attr.ib()


@attr.s
class SetDefault(object):
    value = attr.ib()


@attr.s
class DropDefault(object):
    pass
