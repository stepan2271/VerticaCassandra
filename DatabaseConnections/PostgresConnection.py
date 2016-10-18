import psycopg2
from sqlalchemy import create_engine

from DatabaseConnections.Connections import ConnectionManagement

local_engine = create_engine('postgresql://dbadmin:dbadmin@192.168.16.84:5432/fxet')


class PostgresConnectionManagement(ConnectionManagement):
    def __init__(self):
        super(PostgresConnectionManagement, self).__init__()

    def get_cursor_and_connection(self):
        conn_info = {
            'host': 's-msk-p-fxa-db1',
            'port': 5432,
            'user': 'dbadmin',
            'password': 'dbadmin',
            'database': 'fxet'}
        connection = psycopg2.connect(**conn_info)
        postgres_cursor = connection.cursor()
        return postgres_cursor, connection

    def get_query_delete_table_data(self, table_name, time_column_name):
        return "DELETE FROM " + table_name + " where " + time_column_name + ">=%(start_time)s and " + time_column_name + " <=%(end_time)s;"

    def get_query_delete_same_estimate(self, table_name):
        query = "DELETE FROM " + table_name + " where " + "start_time" + " =%(start_time)s and " + " end_time " + " = %(end_time)s" \
                + " and client_name = %(client_name)s and marination =%(marination)s;"
        return query

    def get_query_delete_only_group(self, table_name, time_column_name):
        query = "DELETE FROM " + table_name + " where " + time_column_name + ">=%(start_time)s and " + time_column_name + " <= %(end_time)s and (pricing_group=%(group)s or pricing_group IS NULL);"
        return query

    def get_query_delete_excluded_dates(self, table_name, time_column_name):
        query = "DELETE FROM " + table_name + " where " + time_column_name + ">= %(start)s and " + time_column_name + "<=%(end)s;"
        return query

    def push_table(self, data, table_name):
        csv_path = self.tmp_path + table_name + '.csv'
        data.to_csv(csv_path, sep=';', index=False, header=None)
        data.to_sql(table_name, local_engine, if_exists='append', index=False)


databases_connection = PostgresConnectionManagement()
postgres_cursor = databases_connection.cursor
postgres_connection = databases_connection.connection
