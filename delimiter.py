import os
import re
import sys
import mysql.connector
from time import sleep
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = os.cpu_count()

def extract_info_from_file(file_path):
    try:
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
                for match in matches:
                    mail, delimiter, password = match
                    mail = mail.strip()
                    password = password.strip()
                    domain = mail.split('@')[1] if '@' in mail else ""
                    table_name = "data_" + mail[:2].lower()
                    query = f"INSERT IGNORE INTO `{table_name}` (mail, password, domain) VALUES (%s, %s, %s)"
                    data = (mail, password, domain)
                    sql_query = query
                    print("Requête SQL avant l'exécution:", sql_query, "avec les données:", data)
                    cursor.execute(query, data)
                db_connection.commit()
            else:
                print(f"Aucun délimiteur commun trouvé dans le fichier: {file_path}\n")
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'insertion : {err}")
        sleep(2)

def analyze_files_in_directory(directory):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    executor.submit(extract_info_from_file, file_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory_to_analyze>")
        sys.exit(1)

    directory_to_analyze = sys.argv[1]
    if not os.path.isdir(directory_to_analyze):
        print(f"Le répertoire '{directory_to_analyze}' n'existe pas.")
        sys.exit(1)

    analyze_files_in_directory(directory_to_analyze)
