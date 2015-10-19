import google.protobuf
from itertools import count


class SchemaObject(dict):
    def __init__(self, *args, **kwargs):
        super(SchemaObject, self).__init__(*args, **kwargs)
        self.__dict__ = self


class Table(SchemaObject):
    pass


def create(branch, name, fields):
    pass












def create_proto(branch, name, fields):
    proto = ["message %s {" % name]
    proto.extend(map(proto_field, zip(fields, count())))
    proto.append("}")
    data = "\n".join(proto).encode('utf8')
    branch['schema/models/%s.proto' % name] = data


#"double" | "float" | "int32" | "int64" | "uint32" | "uint64"
#      | "sint32" | "sint64" | "fixed32" | "fixed64" | "sfixed32" | "sfixed64"
#            | "bool" | "string" | "bytes" | messageType | enumType

FIELD_TYPE_MAP = {
    'BooleanField': 'bool',
    'NullBooleanField': 'bool',
    'AutoField': 'int64',
    'IntegerField': 'sint32',
    'BigIntegerField': 'sint64',
    'SmallIntegerField': 'sint32',
    'PositiveIntegerField': 'uint32',
    'PositiveSmallIntegerField': 'uint32',
    'CharField': 'string',
    'DateField': 'string',
    'TextField': 'string',
    'FloatField': 'float',
    'SlugField': 'string',
    'FileField': 'string',
    'FilePathField': 'string',
    'BinaryField': 'bytes',
    'UUIDField': 'string',
    'DecimalField': 'string',
    'DurationField': 'string',
    'DateTimeField': 'string',
    'TimeField': 'string',
    'GenericIPAddressField': 'string',
}

def proto_field(((name, type, optional, _), i)):
    return '  %s %s %s = %s;' % (
        'optional' if optional else 'required',
        FIELD_TYPE_MAP[type], name, i)
