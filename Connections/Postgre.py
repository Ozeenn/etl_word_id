import psycopg2 as post


class PostgreConnection:
    def __init__(self, credentials):
        self.connection = None
        self.cursor = None
        self.credentials = {
            'user': credentials.user,
            'host': credentials.host,
            'password': credentials.password,
            'database': credentials.database
        }

    def connect(self):
        self.connection = post.connect(**self.credentials)

    def open_cursor(self):
        self.cursor = self.connection.cursor()

    def close_cursor(self):
        self.cursor.close()

    def return_query_list(self, str_query):
        self.cursor.execute(str_query)
        data = self.cursor.fetchone()
        arr_data = []
        while data:
            arr_data.append(data)
            data = self.cursor.fetchone()
        return arr_data

    def insert_into(self, table, data, flg_trunc_before=False):
        conn = post.connect(**self.credentials)
        cur = conn.cursor()

        if flg_trunc_before:
            cur.execute(f'truncate {table}')

        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name ='{name}' order by dtd_identifier".format(name=table.split('.')[1])
        )

        columns = cur.fetchone()
        arr_columns = []
        while columns:
            arr_columns.append(columns[0])
            columns = cur.fetchone()
        insert_columns = f"insert into {table}(" + "".join(f"{column}, " for column in arr_columns if
                                                           column != arr_columns[-1]) + f'{arr_columns[-1]}' + ')'
        insert_values = ','.join(
            cur.mogrify("(" + f"{', '.join('%s' for index in range(len(arr_columns)))}" + ")", i).decode('utf-8') for i
            in data)

        insert_sql = insert_columns + ' values ' + insert_values

        cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()

    def call_proc(self, proc):
        conn = post.connect(**self.credentials)
        cur = conn.cursor()
        cur.execute(
            f'CALL staging.{proc}();'
        )
        conn.commit()
        cur.close()
        conn.close()
