import random
import json

import capnp
capnp.remove_import_hook()


def generate(table):
    if 'proto_id' not in table:
        table['proto_id'] = hex(random.randint(0, 18446744073709551615L))

    proto = "@%s;\n" % table['proto_id']
    proto += "struct %s {\n" % table['name']

    seq = table.setdefault('proto_seq', 0)

    for name in table['columns_order']:
        col = table['columns'][name]
        if 'seq' not in col:
            col['seq'] = seq
            seq += 1
        proto += proto_field(col)

    proto += '}'

    table['proto_seq'] = seq
    table['proto'] = proto.encode('utf8')


def proto_field(field):
    return '  %(name)s @%(seq)s :%(type)s%(default)s;\n' % {
        'name': field['name'],
        'seq': field['seq'],
        'type': FIELD_TYPE_MAP[field['type']],
        'default': json.dumps(field['default']) if 'default' in field else '',
    }


# https://capnproto.org/language.html
#
# Void: Void
# Boolean: Bool
# Integers: Int8, Int16, Int32, Int64
# Unsigned integers: UInt8, UInt16, UInt32, UInt64
# Floating-point: Float32, Float64
# Blobs: Text, Data
# Lists: List(T)

FIELD_TYPE_MAP = {
    'BooleanField': 'Bool',
    'NullBooleanField': 'Bool',
    'AutoField': 'Int64',
    'IntegerField': 'Int32',
    'BigIntegerField': 'Int64',
    'SmallIntegerField': 'Int32',
    'PositiveIntegerField': 'UInt32',
    'PositiveSmallIntegerField': 'UInt32',
    'CharField': 'Text',
    'DateField': 'Text',
    'TextField': 'Text',
    'FloatField': 'float',
    'SlugField': 'Text',
    'FileField': 'Text',
    'FilePathField': 'Text',
    'BinaryField': 'Data',
    'UUIDField': 'Text',
    'DecimalField': 'Text',
    'DurationField': 'Text',
    'DateTimeField': 'Text',
    'TimeField': 'Text',
    'GenericIPAddressField': 'Text',
}
