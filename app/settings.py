from os import getenv
from collections import namedtuple
import discovery
import pdb
pdb.set_trace()

print settings, values

hbase_defaults = {
    'HBASE_HOST_ADDR': '172.17.0.2',
    'HBASE_PORT': '9090',
    'HBASE_TABLE_NAME': 'service_instance',
    'HBASE_DELIM': '-',
    'HBASE_COLUMN_FAMILIES': {'cf0': dict()},
    'BACKEND_STORAGE': 'HBase'
    }

for name, value in defaults.items():
    if isinstance(value, bool):
        values[name] = bool(getenv(name, value))
    elif isinstance(value, int):
        values[name] = int(getenv(name, value))
    elif isinstance(value, basestring):
        values[name] = getenv(name, value)

value = namedtuple('Settings', values.keys())(**values)
print "Value is", value
