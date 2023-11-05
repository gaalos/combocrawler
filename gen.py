import mysql.connector

# Paramètres de connexion à la base de données
config = {
    "user": "XXX",
    "password": "XXX",
    "host": "XXX",
    "database": "XXX",
    "charset": "utf8mb4"
}

# Liste de caractères pour les noms de table
charset = "abcdefghijklmnopqrstuvwxyz0123456789.@-+_~'!?абвгдеёжзийклмнопрстуфхцчшщъыьэюяıİşğğöüçАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯԻՍԳԳՕՒԾԻİŞĞĞÖÜÇ我是中国人"

# Connexion à la base de données
db = mysql.connector.connect(**config)

# Création d'un curseur
cursor = db.cursor()

# Boucle pour créer des tables
for char1 in charset:
    table_name = "data_" + char1
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` (mail VARCHAR(80), password VARCHAR(60), domain VARCHAR(30), UNIQUE KEY unique_constraint (mail, password, domain)) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;"
    cursor.execute(create_table_query)
    for char2 in charset:
        table_name = "data_" + char1 + char2
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` (mail VARCHAR(80), password VARCHAR(60), domain VARCHAR(30), UNIQUE KEY unique_constraint (mail, password, domain)) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;"
        cursor.execute(create_table_query)

# Valider les changements
db.commit()

# Fermer la connexion à la base de données
db.close()
