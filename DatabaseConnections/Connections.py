
class ConnectionManagement(object):
    def __init__(self):
        self.cursor, self.connection = self.get_cursor_and_connection()
        self.tmp_path = "C:\\Temp\\"
        pass
        # self.sdt_ld4 = sqlite3.connect(ld4_deals_home_path)
        # self.sdt_m1 = sqlite3.connect(m1_deals_home_path)

    def get_cursor_and_connection(self):
        raise Exception

    def get_query_insert_table_data(self, table_name, time_column_name):
        raise Exception

    def get_query_delete_excluded_dates(self, table_name, time_column_name):
        raise Exception

    def get_query_delete_only_group(self, table_name, time_column_name):
        raise Exception

    def get_query_delete_same_estimate(self, table_name):
        raise Exception

    def insert_table_data(self, table_name, data, start_time, end_time, time_column_name):
        params = {
            'start_time': start_time,
            'end_time': end_time,}
        query = self.get_query_insert_table_data(table_name, time_column_name)
        self.cursor.execute(query, params)
        self.cursor.fetchall()
        self.push_table(data, table_name)
        self.connection.commit()

    def insert_table_data_delete_same_estimate(self, table_name, data, start_time, end_time, client_name, marination):
        params = {
            'start_time': start_time,
            'end_time': end_time,
            'client_name': client_name,
            'marination': marination
        }
        query = self.get_query_insert_table_delete_same_estimate(table_name)
        self.cursor.execute(query, params)
        self.cursor.fetchall()
        self.push_table(data, table_name)
        self.connection.commit()

    def push_table(self, data, table_name):
        raise Exception

    def get_query_insert_table_delete_same_estimate(self, table_name):
        raise Exception

    def insert_table_data_delete_only_group(self, table_name, data, start_time, end_time, time_column_name, group):
        params = {
            'start_time': start_time,
            'end_time': end_time,
            'group': group}
        query = self.get_query_delete_only_group(table_name, time_column_name)
        self.cursor.execute(query, params)
        self.connection.commit()
        self.push_table(data, table_name)
        self.connection.commit()

