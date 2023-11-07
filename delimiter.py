import os
import re
import sys
import sqlite3
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

MAX_THREADS = 3  # Vous pouvez ajuster ce nombre en fonction de vos ressources
CHUNK_SIZE = 150000000  # Taille du morceau de fichier à lire (en octets)
#CHUNK_SIZE = 15000  # Taille du morceau de fichier à lire (en octets)

def create_table_if_not_exists(cursor, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        mail VARCHAR(80),
        password VARCHAR(60),
        UNIQUE KEY unique_constraint (mail, password)
    ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED
    """
    cursor.execute(create_table_query)

def create_processed_files_table_if_not_exists(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processed_files (
        file_path TEXT
    )
    """)

def is_file_processed(file_path, local_db):
    cursor = local_db.cursor()
    cursor.execute("SELECT file_path FROM processed_files WHERE file_path=?", (file_path,))
    return cursor.fetchone() is not None

def extract_info_from_chunk(chunk, db_connection, local_db_path, file_path, current_chunk, total_chunks):
    pattern = r'([^:;|,\n]+)[:;|,]+(.+)'
    matches = re.findall(pattern, chunk)

    # Obtenir la taille du fichier
    file_size = os.path.getsize(file_path)
    file_chunked = file_size > CHUNK_SIZE

    if matches:
        db_connection = mysql.connector.connect(
            host="XXXX",
            user="XXXX",
            password="XXXX",
            database="test",
            charset="utf8mb4"
        )
        cursor = db_connection.cursor(buffered=True)
        cursor.execute("START TRANSACTION")  # Début de la transaction
        batch_size = 10  # Nombre d'insertions à effectuer avant le commit


        # Créez une barre de progression ici
        progress_bar = tqdm(total=len(matches), unit=" line", desc=f"Traitement de {file_path}")
        if file_chunked:
            progress_bar.set_description(f"Traitement de {file_path} (c {current_chunk}/{total_chunks})")

        variable_compteur = 0
        for match in matches:
            mail, password = match
            mail = mail.strip()
            password = password.strip()
            domain = mail.split('@')[1] if '@' in mail else ""
            table_name = "data_" + mail[:2].lower()
#            create_table_if_not_exists(cursor, table_name)

            while True:
                try:
                    query = f"INSERT IGNORE INTO `{table_name}` (mail, password) VALUES (%s, %s)"
                    data = (mail, password)
                    cursor.execute(query, data)
                    variable_compteur += 1
                    if variable_compteur >= batch_size:
                       db_connection.commit()
                       variable_compteur = 0
                    progress_bar.update(1)  # Mise à jour de la barre de progression
                    break
                except mysql.connector.Error as err:
                    if "Deadlock" in str(err):
                        continue
                    elif "Table 'test." + table_name + "' doesn't exist" in str(err):
                        create_table_if_not_exists(cursor, table_name)

                        query = f"INSERT IGNORE INTO `{table_name}` (mail, password) VALUES (%s, %s)"
                        data = (mail, password)
                        cursor.execute(query, data)
                        progress_bar.update(1)  # Mise à jour de la barre de progression
                        db_connection.commit()
                        continue
                    else:
                        break

        db_connection.commit()  # Commit des insertions restantes
    else:
        print(f"Aucun délimiteur commun trouvé dans le fichier: {file_path}\n")
    progress_bar.update(1)  # Mise à jour de la barre de progression

def process_file(file_path, local_db_path):
