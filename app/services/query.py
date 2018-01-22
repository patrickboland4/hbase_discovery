import abc
import json

from app.services.query import QueryBackend
from plugins.HBase.app.models.host import HappyHost
from plugins.HBase.app.utils import json_utils


class HBaseQueryBackend(QueryBackend):
    import pdb
    print 'setting hbase backend obj'
    pdb.set_trace()
    def __init__(self):
        self.h_host = HappyHost()

    def query(self, service):
        service = str(service)
        service_matches = list(self.h_host.service_table.scan(row_prefix=service))
        for match in service_matches:
            match_json = match[1]['cf0:value']
            host = json.loads(match_json)
            host['last_check_in'] = datetime.datetime.strptime(host['last_check_in'], "%Y-%m-%dT%H:%M:%S.%f")
            yield host

    def query_secondary_index(self, service_repo_name):
        pass

    def get(self, service, ip_address, port):
        row_key = str(self.h_host.create_compound_row_key(service, ip_address, port))
        try:
            result = list(self.h_host.service_table.scan(row_prefix=row_key))
            if result:
                return self._hbase_host_to_dict(self.h_host, service)
            else:
                return None
        except:
            return None

    def _hbase_host_to_dict(self, host, service):
        '''Convert hbase host to a dict'''
        if service is None:
            service = host.service

        _host = {}
        _host['service'] = str(service)
        _host['ip_address'] = str(host.get('ip_address'))
        _host['port'] = str(host.get('port'))
        _host['revision'] = str(host.get('revision'))
        _host['last_check_in'] = str(host.get('last_check_in'))
        _host['tags'] = str(dict([(str(key), str(value)) for key, value in host.get('tags').items()]))
        return _host

    def _prepend_column_family(self, host_dict):
        column_family = self.h_host.column_families
        modified_dict = {}
        for key, value in host_dict.items():
            # come back to this point to process TAGS
            if True:
                modified_dict['{}:{}'.format(column_family, key)] = value
        return modified_dict

    def put(self, host):
        service = host.get('service')
        host_addr = host.get('ip_address')
        port = host.get('port')
        pdb.set_trace()
        host_json = json.dumps(host, cls=DateTimeEncoder)
        compound_row_key = self.h_host.create_compound_row_key(service, host_addr, port)
        self.h_host.service_table.put(row=compound_row_key, data={"cf0:value": host_json})
        return True

    def delete(self, service, ip_address):
        return self.h_host.delete_table(self.h_host.table_name)
