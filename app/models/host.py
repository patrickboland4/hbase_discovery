class HappyHost():
    """
    HBase Host
    """
    def __init__(self):
        self.host_addr = settings.value.HBASE_HOST_ADDR
        self.port = settings.value.HBASE_PORT
        self.table_name = settings.value.HBASE_TABLE_NAME
        self.delimiter = settings.value.HBASE_DELIM
        self.column_families = settings.value.HBASE_COLUMN_FAMILIES
        self.connection = happybase.Connection(host=self.host_addr, port=self.port)

        if self.table_name not in self.connection.tables():
            self.service_table = self.connection.create_table(self.table_name, self.column_families)
        else:
            self.service_table = self.connection.table(self.table_name)

    def establish_connection_with_api(self, **kwargs):
        api_connection = happybase.Connection(**kwargs)
        return api_connection

    def create_compound_row_key(self, service, ip_address, port):
        '''compound_row_key follows convention: service_name<delim>ip<delim>port'''
        #compound_row_key = '{service}{delim}{host_addr}{delim}{port}'.format(
            #service=service, delim=self.h_host.delimiter, host_addr=ip_address, port=int(port))
        compound_row_key = str(service + self.delimiter + ip_address + self.delimiter + str(port))
        return compound_row_key


