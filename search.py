import os
import concurrent.futures
import argparse
import re
import sys
from tqdm import tqdm
import threading

active_threads = []

num_cpus = os.cpu_count()

def find_files_with_pattern(root_path):
    matching_files = []
    num_files_found = 0


    print(f"Recherche des fichiers .txt dans {root_path}...")
    print(f"Nombre de threads CPU disponibles : {num_cpus}")

    def find_files_in_directory(directory):
        nonlocal num_files_found
        try:
            for entry in os.scandir(directory):
                if entry.is_file() and entry.name.endswith('.txt'):
                    matching_files.append(entry.path)
                    num_files_found += 1
                    num_threads = len(active_threads)
                    print(f"Fichiers .txt trouvés jusqu'à présent : {num_files_found} (Threads en cours : {num_threads})", end='\r')
                elif entry.is_dir():
                    find_files_in_directory(entry.path)
        except Exception as e:
            print(f"Erreur lors de la recherche dans le répertoire {directory}: {str(e)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_cpus * 10) as executor:
        for entry in os.scandir(root_path):
            if entry.is_dir():
                future = executor.submit(find_files_in_directory, entry.path)
                active_threads.append(threading.current_thread())

    print("\nFin de la recherche des fichiers .txt.")
    return matching_files

def search_string_in_file(file_path, target_string, output_file_path):
    matching_lines = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line in file:
                if target_string in line:
                    matching_lines.append(line)
        if matching_lines:
            with open(output_file_path, 'a') as output_file:
                output_file.write(f"Chaine de texte '{target_string}' trouvée dans {file_path}.\n")
                output_file.writelines(matching_lines)
    except Exception as e:
        print(f"Erreur lors du traitement du fichier {file_path}: {str(e)}")
    return file_path

def process_file(file, target_string, output_file_path):
#    print(f"Traitement de {file} en cours...")
    try:
        result = search_string_in_file(file, target_string, output_file_path)
#        print(f"Traitement de {result} terminé.")
    except Exception as e:
        print(f"Erreur lors du traitement de {file}: {str(e)}")
    active_threads.remove(threading.current_thread())
    sys.stdout.flush()  # Vide le tampon de la sortie standard

def main(root_directory, target_string, output_file_path):
    matching_files = find_files_with_pattern(root_directory)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_cpus*2) as executor:
        futures = []
        for file in matching_files:
            future = executor.submit(process_file, file, target_string, output_file_path)
            active_threads.append(threading.current_thread())
            futures.append(future)
        
        # Utilisation de tqdm pour afficher une barre de progression globale et indiquer quel fichier est traité
        with tqdm(total=len(futures), desc="Traitement des fichiers") as pbar:
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Recherche une chaîne de texte dans des fichiers texte.')
    parser.add_argument('root_directory', help='Chemin du répertoire de départ.')
    parser.add_argument('target_string', help='La chaîne de texte à rechercher.')
    parser.add_argument('output_file', help='Chemin du fichier de sortie pour les résultats.')
    args = parser.parse_args()

    main(args.root_directory, args.target_string, args.output_file)
