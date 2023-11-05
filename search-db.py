import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import argparse

def search_in_table(table, search_term, output_file_name, pbar):
    try:
        # Établir une connexion à la base de données pour ce thread
        conn = mysql.connector.connect(
            host="XXX",
            user="XXX",
            password="XXX",
            database="XXX",
            charset="utf8mb4"
        )

        cursor = conn.cursor()
        query = f"SELECT * FROM `{table}` WHERE mail LIKE '%{search_term}%'"
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print(f"Trouvé dans la table {table}: {len(results)} correspondance(s)")
            # Écrire les résultats dans le fichier de sortie
            with open(output_file_name, "a", encoding="utf-8") as output_file:
                for row in results:
                    output_file.write("\t".join(str(col) for col in row) + "\n")

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Erreur lors de l'exécution de la requête pour la table {table}: {err}")
    except Exception as e:
        print(f"Erreur non gérée : {e}")

    # Mettre à jour la barre de progression individuelle
    pbar.update(1)

def main():
    # Utilisez argparse pour gérer les options en ligne de commande
    parser = argparse.ArgumentParser(description='Rechercher un mot clé dans les tables de la base de données.')
    parser.add_argument('-w', '--mot-cle', required=False, help='Mot clé à rechercher dans les tables.')
    parser.add_argument('-f', '--fichier-sortie', required=True, help='Fichier de sortie pour stocker les résultats.')
    parser.add_argument('-e', '--mot-exact', action='store_true', help='Rechercher un mot exact (en vérifiant les deux premiers caractères).')

    args = parser.parse_args()
    search_term = args.mot_cle
    output_file_name = args.fichier_sortie
    exact_match = args.mot_exact  # Stockez la valeur de l'option -e

    # Obtenez la liste de toutes les tables dans la base de données
    conn = mysql.connector.connect(
        host="XXX",
        user="XXX",
        password="XXX",
        database="XXX",
        charset="utf8mb4"
    )

    cursor = conn.cursor()

    # Filtrer les noms de table pour correspondre aux deux premiers caractères du mot recherché
    if exact_match and len(search_term) >= 2:
        prefix = search_term[:2]
        # Requête SQL pour obtenir les noms de table correspondant au préfixe
        table_query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'test' AND table_name like 'data_{prefix}'
        """
        cursor.execute(table_query)
        print(f"Recherche exacte data_{prefix}")

        # Récupérez les noms de table
        tables = [table[0] for table in cursor]
    else:
        # Requête SQL pour obtenir les noms de table
        table_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'test' AND table_name LIKE 'data_%'
        """
        cursor.execute(table_query)
        print(f"Recherche multiple")

        # Récupérez les noms de table
        tables = [table[0] for table in cursor]

    # Fermez le curseur
    cursor.close()

    # Créer un fichier de sortie en mode append
    output_file = open(output_file_name, "a", encoding="utf-8")

    # Utilisation de threads pour paralléliser les recherches
    num_tables = len(tables)
    with ThreadPoolExecutor(max_workers=16) as executor:
        # Créer une barre de progression globale
        pbar = tqdm(total=num_tables, desc="Tables traitées")
        for table in tables:
            executor.submit(search_in_table, table, search_term, output_file_name, pbar)

    # Fermez la connexion à la base de données
    conn.close()

    # Fermez le fichier de sortie
    output_file.close()

if __name__ == "__main__":
    main()
