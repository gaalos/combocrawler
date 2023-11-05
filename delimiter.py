import os
import re
import sys
import sqlite3
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

MAX_THREADS = 3

def create_table_if_not_exists(cursor, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        mail VARCHAR(80),
        password VARCHAR(60),
        domain VARCHAR(30),
        UNIQUE KEY unique_constraint (mail, password, domain)
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

def extract_info_from_file(file_path):
    try:
        local_db = sqlite3.connect('local_db.sqlite')
        if not is_file_processed(file_path, local_db):
            with mysql.connector.connect(
                host="XXX",
                user="XXX",
                password="XXX",
                database="XXX",
                charset="utf8mb4"
            ) as db_connection:

                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()

                pattern = r'([^:;|,\n]+)([:;|,])([^:\n]+)'
                matches = re.findall(pattern, content)

                if matches:
                    cursor = db_connection.cursor()
                    commit_count = 0  # Compteur de commit
                    for match in tqdm(matches, desc=f"Traitement de {file_path}"):
                        mail, delimiter, password = match
                        mail = mail.strip()
                        password = password.strip()
                        domain = mail.split('@')[1] if '@' in mail else ""
                        table_name = "data_" + mail[:2].lower()

                        while True:
                            try:
                                query = f"INSERT IGNORE INTO `{table_name}` (mail, password, domain) VALUES (%s, %s, %s)"
                                data = (mail, password, domain)
                                cursor.execute(query, data)
                                commit_count += 1
                                if commit_count >= 200 or len(matches) <= 200:
                                    db_connection.commit()  # Faire un commit tous les 200 insertions ou à la fin du fichier
                                    commit_count = 0  # Réinitialiser le compteur
                                break
                            except mysql.connector.Error as err:
                                if "Deadlock" in str(err):
                                    print(f"Deadlock détecté, réessai de la requête...")
                                    continue
                                elif "Table 'test." + table_name + "' doesn't exist" in str(err):
                                    print(f"La table {table_name} n'existe pas, en train de la créer...")
                                    create_table_if_not_exists(cursor, table_name)
                                    continue
                                else:
                                    print(f"Erreur lors de l'insertion : {err}")
                                    break

                    if commit_count > 0:
                        db_connection.commit()  # Commit des insertions restantes
                    local_db.execute("INSERT INTO processed_files (file_path) VALUES (?)", (file_path,))
                    local_db.commit()
                else:
                    print(f"Aucun délimiteur commun trouvé dans le fichier: {file_path}\n")
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à la base de données : {err}")
    except Exception as e:
        print(f"Erreur lors du traitement du fichier {file_path}: {str(e)}")

def analyze_files_in_directory(directory):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    if not is_file_processed(file_path, sqlite3.connect('local_db.sqlite')):
                        executor.submit(extract_info_from_file, file_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory_to_analyze>")
        sys.exit(1)

    directory_to_analyze = sys.argv[1]
    if not os.path.isdir(directory_to_analyze):
        print(f"Le répertoire '{directory_to_analyze}' n'existe pas.")
        sys.exit(1)

    local_db = sqlite3.connect('local_db.sqlite')
    create_processed_files_table_if_not_exists(local_db)
    analyze_files_in_directory(directory_to_analyze)
