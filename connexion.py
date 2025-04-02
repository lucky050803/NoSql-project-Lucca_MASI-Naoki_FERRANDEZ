from pymongo import MongoClient
from neo4j import GraphDatabase
from config import MONGO_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def connect_mongo():
    """Établit une connexion à MongoDB et retourne la base de données."""
    client = MongoClient(MONGO_URI)
    return client["entertainment"]

def connect_neo4j():
    """Établit une connexion à Neo4j et retourne un driver."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD),database="films")

if __name__ == "__main__":
    try:
        db = connect_mongo()
        print("Connexion MongoDB réussie.")
    except Exception as e:
        print(f"Erreur de connexion MongoDB : {e}")

    try:
        driver = connect_neo4j()
        with driver.session() as session:
            result = session.run("RETURN 'Connexion Neo4j réussie' AS message")
            print(result.single()["message"])
    except Exception as e:
        print(f"Erreur de connexion Neo4j : {e}")