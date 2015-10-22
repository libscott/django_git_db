import datetime
import json
import random
import tempfile
import time

import capnp
capnp.remove_import_hook()


def generate(table):
    if 'proto_id' not in table:
        r = random.randint(0, 1<<63) | 1<<63
        table['proto_id'] = hex(r).rstrip('L')

    proto = "@%s;\n" % table['proto_id']
    uname = make_title_name(table['name'])
    proto += "struct %s {\n" % uname

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
    'DateTimeField': 'UInt64',
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
    'TimeField': 'Text',
    'GenericIPAddressField': 'Text',
}


def proto_field(field):
    return '  %(name)s @%(seq)s :%(type)s%(default)s;\n' % {
        'name': field['name'],
        'seq': field['seq'],
        'type': FIELD_TYPE_MAP[field['type']],
        'default': json.dumps(field['default']) if 'default' in field else '',
    }


def make_title_name(name):
    return str(''.join(p.title() for p in name.split('_')))


PROTO_CACHE = {}


def get_serializer(branch, table_name):
    title = make_title_name(table_name)
    key = 'tables/%s/schema.json' % table_name
    schema_id = branch.tree.object_id(key)
    if schema_id not in PROTO_CACHE:
        table = json.loads(branch[key])
        with tempfile.NamedTemporaryFile() as f:
            f.write(table['proto'])
            f.flush()
            module = capnp.load(f.name)
        cls = type(title + 'Proto', (Proto,), {})
        proto = cls(table, getattr(module, title))
        PROTO_CACHE[schema_id] = proto
    return PROTO_CACHE[schema_id]


class Proto(object):
    def __init__(self, schema, model):
        self.schema = schema
        self.model = model

    def from_dict(self, obj):
        for k, v in obj.items():
            if type(v) == datetime.datetime:
                obj[k] = int(time.mktime(v.timetuple()))
        return self.model.from_dict(obj)

