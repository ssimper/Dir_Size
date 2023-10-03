# source : https://www.sqlitetutorial.net/sqlite-python/creating-database/
# source racine : https://www.sqlitetutorial.net/sqlite-python/

import datetime, sqlite3
from datetime import datetime
from sqlite3 import Error

my_db = "/home/simper/Dropbox/Etudes/Python/Projets_réels/Dir_Size/Prod/siah_data.db"


def create_connection(db_file):
    """Create database connexion to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    return conn
    

def show_cols(conn, table_name):
    cols_list = []
    sql = """PRAGMA table_info(""" + table_name + """)"""
    cur = conn.cursor()
    cur.execute(sql)
    records = cur.fetchall()
    for record in records:
        if records.index(record) > 1:
            cols_list.append(record[1])
    print(cols_list)


def check_tables(conn, db_file):
    cursor = conn.cursor()
    sql = """
        SELECT name
        FROM sqlite_schema
        WHERE type ='table' AND name NOT LIKE 'sqlite_%'
    """
    result = cursor.execute(sql)
    table_list = [elt[0] for elt in result]
    return table_list


def compare_records(conn, table_list):
    cursor = conn.cursor()
    #Each table is a share
    for table in table_list:
        print(f'Partage : {table}')
        #First record
        sql_first = """SELECT * FROM """ + table + """ ORDER BY id LIMIT 1"""
        result_first = cursor.execute(sql_first)
        #First record
        for row in result_first:
            pass
        first_records = (list(row))[2:]
        #Extract columns name
        cols_name = [col[0] for col in result_first.description]
        cols_name = cols_name[2:]
        #Last record
        sql_last = """SELECT * FROM """ + table + """ ORDER BY id DESC LIMIT 1"""
        result_last = cursor.execute(sql_last)
        for row in result_last:
            pass
        last_records = (list(row))[2:]
        last_id = row[0]
        #Yesterday's record
        yesterday = last_id - 6
        sql_yest = """SELECT * FROM """ + table + """ WHERE id = '""" + str(yesterday) + """'"""
        result_yest = cursor.execute(sql_yest)
        for row in result_yest:
            pass
        yesterday_records = (list(row))[2:]
        my_result = [
            (
                share_name, int(initial_size),
                int(yesterday_size),
                int(today_size)
            ) for share_name, initial_size, yesterday_size, today_size in zip(
                cols_name,
                first_records,
                yesterday_records,
                last_records
            )
        ]
        my_result.sort(key=lambda x: x[3], reverse=True)
        for result in my_result:
            current_dir = result[0]
            first_rec = result[1]
            yest_rec = result[2]
            last_rec = result[3]
            size_var_percent = ((int(last_rec) - int(yest_rec)) / (int(last_rec))*100)
            size_var_percent = round(size_var_percent, 2)
            print(
                f'{table}>{current_dir}, Initiale : {first_rec} | ' \
                f'N-7 : {yest_rec} | N : {last_rec} | variation : {size_var_percent}%'
            )


def dir_size(conn, table):
    cursor = conn.cursor()
    sql = """SELECT * FROM """ + table
    result = cursor.execute(sql)
    id_pos = 0
    for row in result:
        row = list(row)
        folders_size = row[2:]
        total_size = 0
        for folder_size in folders_size:
            total_size += int(folder_size)
        if id_pos == 0:
            initial_size = total_size
        variation = ((total_size - initial_size) / initial_size) * 100
        variation = round(variation, 2)
        datetime_object = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S.%f')
        print(table, datetime_object.strftime("%Y-%m-%d"), total_size, initial_size, variation)
        id_pos += 1




def main():
    conn = create_connection(my_db)
    table_list = check_tables(conn, my_db)
    print(table_list)
    #compare_records(conn, table_list)
    for table in table_list:
        dir_size(conn, table)
    conn.close()
     



if __name__ == '__main__':
     main()
    

    