import datetime,sqlite3,subprocess
from constants import my_db, parent_directories
from pathlib import Path
from sql_commands import add_columns, create_connection, create_table, \
    data_entry, remove_columns, show_tables
from sqlite3 import Error




def folders_list(parent_folder):
    """Create a list of tuple (subfolders name, subfolders size)

    Returns:
        parent_folder_name (variable): name of the parenet folder
        subfolders_name_size : (list): list of tuples (sub-folders, size)
    """
    forbidden_char = [" ", "-", "*", "&", "'", "(", ")", "+", "\\", "/"]
    parent_folder_name = parent_folder.name
    print('Traitement du dossier ', parent_folder_name)
    subfolders_name_size = []
    #Listing the sub-directories (not the masked one)
    folders_list = [
        d for d in parent_folder.iterdir() if d.is_dir() and "." not in d.name
    ]

    #ORdering list
    folders_list.sort()
    #Size of each sub-directories
    for folder in folders_list:
        result = subprocess.check_output(
            ['du', '-s', '--block-size=1M', str(folder)]
        )
        current_folder_size = result.decode('utf-8').split()[0]
        treated_folder_name = folder.name
        #If first char is a decimal we add '_' before
        if treated_folder_name[0].isdecimal():
            treated_folder_name = "_" + treated_folder_name
        #If a forbiden char in name, replace it with '_'
        for char in treated_folder_name:
            if char in forbidden_char:
                treated_folder_name = treated_folder_name.replace(char, "_")
        #If more than 40 chars in name we trucate
        if len(treated_folder_name) > 40:
            treated_folder_name = treated_folder_name[:40]
        subfolders_name_size.append(
            (folder.name, treated_folder_name, current_folder_size)
        )
    return parent_folder_name, subfolders_name_size


def table_conscistency(
        conn, table_name, columns_list,
        parent_folder_name, subfolders_name_size
    ):
    """Check DB columns versus subfolders name. If there is a difference
    columns will be removed or added.

    Args:
        conn (sqlite3): DB connection
        table_name (variable): Name of the table
        columns_list (list): list of currents columns
        subfolders_name_size (list)): list of subfolders
    """
    #Just the list of treated folders name
    treated_subfolders_name = [elt[1] for elt in subfolders_name_size]
    if table_name == "":
        print("No table. Must create it.")
        create_table(conn, parent_folder_name, treated_subfolders_name)
    else:
        #Remove 2 first colomns (id and date) to match parent dir structure
        cols_as_folders = columns_list[2:]
        #Order all lists
        cols_as_folders.sort()
        treated_subfolders_name.sort()
        print(
            'La table ', table_name, ' a été trouvée. On vérifie les colonnes ...'
        )
        #Columns exists ?
        if len(columns_list) == 0:
            print("Pas de colonnes ...")
        #Columns like subfolders ?
        elif treated_subfolders_name == cols_as_folders:
            print("Tout est égale.")
        else:
            print("Il y a une différence.")
            #List of folders added
            folder_added = [
                elt for elt in treated_subfolders_name \
                if elt not in cols_as_folders
            ]
            #If list not null we add equivalent cols in table
            if len(folder_added) != 0:
                print('Dossier(s) ajouté(s) :', ", ".join(folder_added))
                add_columns(conn,table_name,folder_added)
            #List of oflders removed
            folder_removed = [
                elt for elt in cols_as_folders \
                    if elt not in treated_subfolders_name
                ]
            #If list not null we remove equivalent cols in table 
            if len(folder_removed) != 0:
                print("Dossier(s) retiré(s) :", ", ".join(folder_removed))
                new_col_list = [
                    col_name for col_name in treated_subfolders_name \
                    if col_name not in folder_removed
                ]
                new_col_list.insert(0, "Date")
                remove_columns(conn, table_name, new_col_list)
                print("Colonne(s) retirée(s) :", ", ".join(folder_removed))




def the_date():
    today_full = datetime.datetime.now()
    today_light = today_full.strftime('%Y-%m-%d')
    print("Nous sommes le , ", today_light)
    return today_full



def main():
    today = the_date()
    for directory in parent_directories:
        if parent_directories.index(directory) > 0:
            print("**********")
        #Get parent folder name and list of sub-folders
        parent_folder_name, subfolders_name_size = folders_list(directory)
        if len(subfolders_name_size) != 0:
            #create_connection(my_db)
            conn = create_connection(my_db)
            table_name, columns_list = show_tables(conn, parent_folder_name)
            table_conscistency(
                conn, table_name, columns_list,
                parent_folder_name, subfolders_name_size
            )
            data_entry(conn, today, parent_folder_name, subfolders_name_size)
            conn.close()
        else:
            print(f"Le dossier {parent_folder_name} est vide, on passe ...")
    


if __name__ == '__main__':
    main()