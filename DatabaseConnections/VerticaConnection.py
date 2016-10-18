import vertica_python

from DatabaseConnections.Connections import ConnectionManagement


class VerticaConnectionManagement(ConnectionManagement):
    def __init__(self):
        super(VerticaConnectionManagement, self).__init__()
        # self.sdt_ld4 = sqlite3.connect(ld4_deals_home_path)
        # self.sdt_m1 = sqlite3.connect(m1_deals_home_path)

    def get_cursor_and_connection(self):
        conn_info = {
            'host': 's-msk-t-fxa-ls1',
            'port': 5433,
            'user': 'dbadmin',
            'password': 'aBc524930',
            'database': 'prices'}
        connection = vertica_python.connect(**conn_info)
        vertica_cursor = connection.cursor()
        return vertica_cursor, connection

    def get_query_delete_table_data(self, table_name, time_column_name):
        return "DELETE FROM " + table_name + " where " + time_column_name + ">=:start_time and " + time_column_name + " <= :end_time;"

    def get_query_delete_same_estimate(self, table_name):
        query = "DELETE FROM " + table_name + " where " + "start_time" + " =:start_time and " + " end_time " + " = :end_time" \
                + " and client_name = :client_name and marination =:marination;"
        return query

    def get_query_delete_only_group(self, table_name, time_column_name):
        query = "DELETE FROM " + table_name + " where " + time_column_name + ">=:start_time and " + time_column_name + " <= :end_time and (PricingGroup=:group or PricingGroup IS NULL);"
        return query

    def get_query_delete_excluded_dates(self, table_name, time_column_name):
        query = "DELETE FROM " + table_name + " where " + time_column_name + ">= :start and " + time_column_name + "<= :end;"
        return query

        # def get_vertica_batch_insert(self, columns_list, table_name, truncate=False):
        #     batch = VerticaBatch(
        #         odbc_kwargs={'DRIVER': "HPVertica", 'SERVER': "localhost",
        #                      'DATABASE': "prices", 'PORT': 5433, 'UID': 'dbadmin',
        #                      'PWD': 'aBc524930'},
        #         table_name=table_name,
        #         truncate_table=truncate,
        #         column_list=columns_list,
        #         copy_options={
        #             'DELIMITER': ',',
        #         },
        #         multi_batch=True
        #     )
        #     return batch

    def push_table(self, data, table_name):
        csv_path = self.tmp_path + table_name + '.csv'
        data.to_csv(csv_path, sep=';', index=False, header=None)
        with open(csv_path) as file:
            self.cursor.copy("COPY " + table_name + " from stdin DELIMITER ';' ", file)


databases_connection = VerticaConnectionManagement()
vertica_cursor = databases_connection.cursor
vertica_connection = databases_connection.connection
