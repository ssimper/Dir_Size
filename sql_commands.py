import sqlite3
from constants import my_db
import datetime
from sqlite3 import Error



def create_connection(db_file):
    """Create database connexion to a SQLite database
    specified by db_file.
    Source for date insertion :
    https://pynative.com/python-sqlite-date-and-datetime/
    :param db_file: database file
    :return: Connection object or None"""

    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES |
                        sqlite3.PARSE_COLNAMES)
        print(f'Version SQLite : {sqlite3.version}')
        return conn
    except Error as e:
        print(e)
    return conn


def show_tables(conn, parent_folder_name):
    """Verify if table nameds like parent folder exists.
       If exists, verify if list of colums equal list of subfolder
       :param parent_folder_name: name of the table
       :return: table_name, list of columns"""
    table_name = ""
    columns_name = []
    sql = """
        SELECT
            name
        FROM
            sqlite_schema
        WHERE
            type = 'table'
        AND
            name NOT LIKE 'sqlite_%';
    """
    try:
        c = conn.cursor()
        result = c.execute(sql)
        #fetchall return a list of tuples (table, columns)
        result2 = result.fetchall()
    except Error as e:
        print(e)
    if len(result2) != 0:
        #List of only tables names
        table_list = [table[0] for table in result2]
        #If parent dolfer allready in DB we sort columns
        if parent_folder_name in table_list:
            table_name = parent_folder_name
            result3 = (
                c.execute("""
                          PRAGMA table_info(""" + table_name + """);
                """)
            ).fetchall()
            for col in result3:
                columns_name.append(col[1])
    return table_name, columns_name
        

def add_columns(conn, table, columns_to_add):
    cur = conn.cursor()
    for column in columns_to_add:
        sql = "ALTER TABLE " + table + \
            " ADD COLUMN "+ column + \
            " VARCHAR(20) DEFAULT '0' NOT NULL"
        cur.execute(sql)
    print("Colonne(s) ajoutée(s) :", ", ".join(columns_to_add))


def remove_columns(conn, table, new_col_list):
    cur = conn.cursor()
    #https://www.sqlitetutorial.net/sqlite-alter-table/
    #https://www.geeksforgeeks.org/how-to-execute-many-sqlite-statements-in-python/
    
    # step 1 : create a new table
    new_table = table + "_new"
    create_table(conn, new_table, new_col_list)
    # step 2 : Copy data from old to new table
    print("Copying datas ...")
    print(new_col_list)
    col_list = ", ".join(new_col_list)
    sql = """
        INSERT INTO """ + new_table + """(""" + col_list + """) SELECT """ \
        + col_list + """ FROM """ + table + """;
    """
    print(sql)
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)
    # step 3 remove old table
    print(f'Removing table {table}...')
    sql = """DROP TABLE """ + table + """;"""
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)
    # step 4 rename table
    print(f'Renaming table {new_table} as {table}')
    sql = """ALTER TABLE """ + new_table + """ RENAME TO """ + table + """;"""
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)
    #step 5 commit the transaction
    sql = """COMMIT;"""
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)
    pass


def create_table(conn, parent_folder_name, child_folder_list):
    """Create table.
       'parent_folder_name' is the name of the table.
       'child_folder_list' values are the sub-directories, will be
        the columns name

    Args:
        conn (sqlite3 connection): 
        parent_folder_name : name to use for the table
        child_folder_list (list) : list of the sub-directories
    """
    table_name = parent_folder_name
    #folders_list = [elt[0] for elt in child_folder_list]
    #Order list
    child_folder_list.sort()
    print(f"Creating table {table_name}...")
    sql_create_table = """ CREATE TABLE IF NOT EXISTS """ + table_name + """
        (
            id integer PRIMARY KEY,
            Date TIMESTAMP
        );
    """
    try:
        c = conn.cursor()
        c.execute(sql_create_table)
    except Error as e:
        print(e)
    for folder in child_folder_list:
        sql_alter_table = "ALTER TABLE " + table_name + \
            " ADD COLUMN "+ folder + " VARCHAR(20) "
        try:
            c = conn.cursor()
            c.execute(sql_alter_table)
        except Error as e:
            print(e)
        if "_new" not in table_name:
            print("added column", folder, "to", table_name, "table -- > ok")
    #print(table_name, folders_list)


def data_entry(conn, today, table_name, subfolders_name_size):
    #https://stackoverflow.com/questions/43240617/python-sqlite3-insert-list
    print("Traitement de la table ", table_name)
    #print(
    #    table_name, columns_name, folders_size_list,
    #    len(columns_name), len(folders_size_list))
    cols_name_list = [value[1] for value in subfolders_name_size]
    cols_name = "Date," + ",".join(cols_name_list)
    cols_values_list = [value[2] for value in subfolders_name_size]
    cols_values_list.insert(0, str(today))
    data_tuple = tuple(cols_values_list)
    cur = conn.cursor()
    #sql = "INSERT INTO " + table_name + "(" + cols_name + ") VALUES(" + cols_values + ");"
    sql = "INSERT INTO " + table_name + "(" + cols_name + ") VALUES" + str(data_tuple) + ";"
    print("sql = ", sql)
    cur.execute(sql)
    conn.commit()


def read_data(conn):
    sql = """SELECT Date FROM Documents"""
    cur = conn.cursor()
    cur.execute(sql)
    records = cur.fetchall()
    for record in records:
        print(record[0].strftime("%d-%m-%Y %H:%M"))

def main():
    conn = create_connection(my_db)
    read_data(conn)


if __name__ == '__main__':
    main()
