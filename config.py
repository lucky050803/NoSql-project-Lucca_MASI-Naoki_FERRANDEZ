import configparser

# Charger la configuration depuis le fichier config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# Récupérer les informations de connexion aux bases de données
MONGO_URI = config["DATABASE"]["MONGO_URI"]
NEO4J_URI = config["DATABASE"]["NEO4J_URI"]
NEO4J_USER = config["DATABASE"]["NEO4J_USER"]
NEO4J_PASSWORD = config["DATABASE"]["NEO4J_PASSWORD"]