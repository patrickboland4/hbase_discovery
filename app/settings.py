from collections import namedtuple
from os import getenv

hbase_defaults = {
    'HBASE_HOST_ADDR': '172.17.0.2',
    'HBASE_PORT': '9090',
    'HBASE_TABLE_NAME': 'service_instance',
    'HBASE_DELIM': '-',
    'HBASE_COLUMN_FAMILIES': {'cf0': dict()},
    'BACKEND_STORAGE': 'HBase'
    }

values = {}
for name, value in hbase_defaults.items():
    values[name] = value

value = namedtuple('Settings', values.keys())(**values)
