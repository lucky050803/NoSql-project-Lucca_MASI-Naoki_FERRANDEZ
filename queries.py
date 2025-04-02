# mongo_queries.py

from pymongo.collection import Collection
from typing import List, Dict, Any
from pprint import pprint

# 1. Année avec le plus de films sortis
def most_common_year(films: Collection):
    return films.aggregate([
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ])

# 2. Nombre de films après 1999
def count_movies_after_1999(films: Collection) -> int:
    return films.count_documents({"year": {"$gt": 1999}})

# 3. Moyenne des votes pour 2007
def average_votes_2007(films: Collection):
    return films.aggregate([
        {"$match": {"year": 2007}},
        {"$group": {"_id": None, "average_votes": {"$avg": "$Votes"}}}
    ])

# 4. Histogramme : films par année
def movies_per_year(films: Collection):
    return films.aggregate([
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])

# 5. Genres disponibles
def available_genres(films: Collection) -> List[str]:
    return films.distinct("genre")

# 6. Film avec le plus de revenus
def highest_revenue_movie(films: Collection):
    return films.find({
        "Revenue (Millions)": {
            "$ne": None,
            "$ne": ""
        }
    }).sort("Revenue (Millions)", -1).limit(1)


# 7. Réalisateurs avec plus de 5 films
def prolific_directors(films: Collection):
    return films.aggregate([
        {"$match": {"Director": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])


# 8. Genre le plus rentable en moyenne
def most_profitable_genre(films: Collection):
    return films.aggregate([
        {"$unwind": "$genre"},
        {"$group": {"_id": "$genre", "average_revenue": {"$avg": "$Revenue (Millions)"}}},
        {"$sort": {"average_revenue": -1}},
        {"$limit": 1}
    ])

# 9. Top 3 films notés par décennie
def top_3_movies_per_decade(films: Collection):
    return films.aggregate([
        {"$addFields": {"decade": {"$concat": [{"$toString": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}, "s"]}}},
        {"$sort": {"decade": 1, "Metascore": -1}},
        {"$group": {
            "_id": "$decade",
            "top_films": {"$push": {"title": "$title", "Metascore": "$Metascore"}}
        }},
        {"$project": {"top_3": {"$slice": ["$top_films", 3]}}}
    ])

# 10. Film le plus long par genre
def longest_movie_by_genre(films: Collection):
    return films.aggregate([
        {"$unwind": "$genre"},
        {"$sort": {"Runtime (Minutes)": -1}},
        {"$group": {
            "genre": "$genre",
            "film": {"$first": "$title"},
            "Runtime (Minutes)": {"$first": "$Runtime (Minutes)"}
        }}
    ])

# 11. Films avec Metascore > 80 et Revenue > 50M
def filtered_view(films: Collection):
    return films.find({
        "Metascore": {"$gt": 80},
        "Revenue (Millions)": {"$gt": 50}
    })

# 12. Runtime vs Revenue (récupération)
from typing import Collection
import pandas as pd
from scipy.stats import pearsonr

def runtime_vs_revenue_data(films: Collection):
    pipeline = [
        {
            "$project": {
                "runtime": {
                    "$convert": {
                        "input": "$Runtime (Minutes)",
                        "to": "int",
                        "onError": None,
                        "onNull": None
                    }
                },
                "revenue": {
                    "$convert": {
                        "input": "$Revenue (Millions)",
                        "to": "double",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {
            "$match": {
                "runtime": {"$ne": None},
                "revenue": {"$ne": None}
            }
        }
    ]

    data = list(films.aggregate(pipeline))
    if not data:
        print("Aucune donnée valide trouvée.")
        return None

    df = pd.DataFrame(data).dropna()

    if df.empty:
        print("Pas assez de données après filtrage.")
        return None

    corr, p_value = pearsonr(df["runtime"], df["revenue"])

    print("Corrélation entre la durée et le revenu :")
    print(f"Corrélation de Pearson : {corr:.4f}")
    print(f"P-value : {p_value:.4g}")

    return ""



# 13. Durée moyenne par décennie
def average_runtime_by_decade(films: Collection):
    return films.aggregate([
        {"$addFields": {"decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}},
        {"$group": {
            "_id": "$decade",
            "average_runtime": {"$avg": "$Runtime (Minutes)"}
        }},
        {"$sort": {"_id": 1}}
    ])




# neo4j_queries.py

from neo4j import Driver

def run_query(driver: Driver, query: str):
    with driver.session() as session:
        return list(session.run(query))

def most_featured_actor(driver: Driver):
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    RETURN a.name AS actor, COUNT(f) AS films
    ORDER BY films DESC
    LIMIT 1
    """
    return run_query(driver, query)

def coactors_with_anne_hathaway(driver: Driver):
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(anne:Actor {name: "Anne Hathaway"})
    WHERE a.name <> "Anne Hathaway"
    RETURN DISTINCT a.name AS actor
    """
    return run_query(driver, query)

def highest_grossing_actor(driver: Driver):
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    WHERE f.revenue IS NOT NULL
    RETURN a.name AS actor, SUM(f.revenue) AS total_revenue
    ORDER BY total_revenue DESC
    LIMIT 1
    """
    return run_query(driver, query)

def average_votes(driver: Driver):
    query = """
    MATCH (f:Film)
    WHERE f.votes IS NOT NULL
    RETURN AVG(f.votes) AS average_votes
    """
    return run_query(driver, query)

def most_common_genre(driver: Driver):
    query = """
    MATCH (f:Film)-[:A_UN_GENRE]->(g:Genre)
    RETURN g.name AS genre, COUNT(f) AS count
    ORDER BY count DESC
    LIMIT 1
    """
    return run_query(driver, query)

def movies_with_group_members(driver: Driver, member_names: list):
    names_str = ', '.join(f'"{name}"' for name in member_names)
    query = f"""
    MATCH (m:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE m.name IN [{names_str}]
    RETURN DISTINCT f.title AS film
    """
    return run_query(driver, query)

def director_with_most_unique_actors(driver: Driver):
    query = """
    MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    RETURN r.name AS director, COUNT(DISTINCT a) AS unique_actors
    ORDER BY unique_actors DESC
    LIMIT 1
    """
    return run_query(driver, query)

def most_connected_films(driver: Driver):
    query = """
    MATCH (f:Film)<-[:A_JOUE]-(a:Actor)-[:A_JOUE]->(other:Film)
    WHERE f <> other
    RETURN f.title AS film, COUNT(DISTINCT other) AS connections
    ORDER BY connections DESC
    LIMIT 5
    """
    return run_query(driver, query)

def actors_with_most_directors(driver: Driver):
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_REALISE]-(r:Realisateur)
    RETURN a.name AS actor, COUNT(DISTINCT r) AS director_count
    ORDER BY director_count DESC
    LIMIT 5
    """
    return run_query(driver, query)

def recommend_by_genre(driver: Driver, actor_name: str):
    query = f"""
    MATCH (a:Actor {{name: "{actor_name}"}})-[:A_JOUE]->(:Film)-[:A_UN_GENRE]->(g:Genre)
    WITH a, COLLECT(DISTINCT g.name) AS genres
    MATCH (f:Film)-[:A_UN_GENRE]->(g2:Genre)
    WHERE g2.name IN genres AND NOT (a)-[:A_JOUE]->(f)
    RETURN DISTINCT f.title AS recommended_film
    LIMIT 5
    """
    return run_query(driver, query)

def influence_between_directors(driver: Driver):
    query = """
    MATCH (r1:Realisateur)-[:A_REALISE]->(:Film)-[:A_UN_GENRE]->(g:Genre)<-[:A_UN_GENRE]-(:Film)<-[:A_REALISE]-(r2:Realisateur)
    WHERE r1 <> r2
    MERGE (r1)-[:INFLUENCE_PAR]->(r2)
    RETURN DISTINCT r1.name, r2.name
    """
    return run_query(driver, query)

def shortest_path_between_actors(driver: Driver, actor1: str, actor2: str):
    query = f"""
    MATCH p = shortestPath((a1:Actor {{name: "{actor1}"}})-[:A_JOUE*]-(a2:Actor {{name: "{actor2}"}}))
    RETURN p
    """
    return run_query(driver, query)

def actor_communities(driver: Driver):
    # Étape 1 : créer les relations COJOUE
    query_create_cojoue = """
    MATCH (a1:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a2:Actor)
    WHERE a1.name < a2.name
    WITH a1, a2, COUNT(f) AS films_together
    WHERE films_together >= 2
    MERGE (a1)-[r:COJOUE]-(a2)
    SET r.score = films_together
    """

    # Étape 2 : détecter les communautés (avec fix WHERE)
    query_detect_communities = """
    MATCH (a1:Actor)
    WITH collect(a1) AS actors
    UNWIND actors AS a1
    UNWIND actors AS a2
    WITH a1, a2
    WHERE a1 <> a2
    MATCH p = shortestPath((a1)-[:COJOUE*]-(a2))
    WITH a1.name AS acteur_1, collect(DISTINCT a2.name) AS community
    RETURN acteur_1, community
    ORDER BY size(community) DESC
    LIMIT 20
    """

    try:
        run_query(driver, query_create_cojoue)
    except Exception as e:
        print("⚠️ Erreur lors de la création des relations COJOUE :", e)

    try:
        return run_query(driver, query_detect_communities)
    except Exception as e:
        print("⚠️ Erreur lors de la détection des communautés :", e)
        return []


# cross_queries.py

from neo4j import Driver

def run_query(driver: Driver, query: str):
    with driver.session() as session:
        return list(session.run(query))

# 27. Films qui ont des genres en commun mais des réalisateurs différents
def similar_genre_diff_director(driver: Driver):
    query = """
    MATCH (f1:Film)-[:A_UN_GENRE]->(g:Genre)<-[:A_UN_GENRE]-(f2:Film),
          (f1)<-[:A_REALISE]-(d1:Realisateur),
          (f2)<-[:A_REALISE]-(d2:Realisateur)
    WHERE f1 <> f2 AND d1 <> d2
    RETURN DISTINCT f1.title AS Film1, d1.name AS Real1, 
                    f2.title AS Film2, d2.name AS Real2, g.name AS Genre
    LIMIT 20
    """
    return run_query(driver, query)

# 28. Recommander des films à un utilisateur selon les préférences d’un acteur
def recommend_to_user_based_on_actor(driver: Driver, actor_name: str):
    query = f"""
    MATCH (a:Actor {{name: "{actor_name}"}})-[:A_JOUE]->(:Film)-[:A_UN_GENRE]->(g:Genre)
    WITH a, COLLECT(DISTINCT g.name) AS preferred_genres
    MATCH (f:Film)-[:A_UN_GENRE]->(g2:Genre)
    WHERE g2.name IN preferred_genres AND NOT (a)-[:A_JOUE]->(f)
    RETURN DISTINCT f.title AS recommendation, COLLECT(DISTINCT g2.name) AS matching_genres
    LIMIT 10
    """
    return run_query(driver, query)

# 29. Relations de concurrence entre réalisateurs (films similaires, même année)
def create_competition_relationships(driver: Driver):
    query = """
    MATCH (f1:Film)<-[:A_REALISE]-(d1:Realisateur),
          (f2:Film)<-[:A_REALISE]-(d2:Realisateur),
          (f1)-[:A_UN_GENRE]->(g:Genre)<-[:A_UN_GENRE]-(f2)
    WHERE f1 <> f2 AND f1.year = f2.year AND d1 <> d2
    MERGE (d1)-[:EN_CONCURRENCE_AVEC]->(d2)
    RETURN DISTINCT d1.name AS Director1, d2.name AS Director2, f1.year AS Year, g.name AS Genre
    LIMIT 20
    """
    return run_query(driver, query)

# 30. Collaborations fréquentes entre réalisateurs et acteurs + succès commercial ou critique
def frequent_collabs_and_success(driver: Driver):
    query_create_collabs = """
    MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE f.`Revenue (Millions)` IS NOT NULL AND f.`Revenue (Millions)` <> ""
    OR f.Metascore IS NOT NULL AND f.Metascore <> ""
    WITH r, a,
        COUNT(DISTINCT f) AS nb_collabs,
        SUM(
            CASE WHEN f.`Revenue (Millions)` IS NOT NULL AND f.`Revenue (Millions)` <> "" 
                THEN toFloat(f.`Revenue (Millions)`) ELSE 0 END
        ) AS total_revenue,
        SUM(
            CASE WHEN f.Metascore IS NOT NULL AND f.Metascore <> "" 
                THEN toInteger(f.Metascore) ELSE 0 END
        ) AS total_metascore
    MERGE (r)-[c:COLLAB_AVEC]->(a)
    SET c.count = nb_collabs,
        c.total_revenue = total_revenue,
        c.total_metascore = total_metascore
    """
    query_extract_top = """
    MATCH (r:Realisateur)-[c:COLLAB_AVEC]->(a:Actor)
    WHERE c.count >= 2
    RETURN 
        r.name AS realisateur,
        a.name AS acteur,
        c.count AS nb_collaborations,
        round(c.total_revenue / c.count, 2) AS revenue_moyen,
        round(c.total_metascore / c.count, 1) AS metascore_moyen
    ORDER BY nb_collaborations DESC, revenue_moyen DESC
    LIMIT 20
    """

    try:
        run_query(driver, query_create_collabs)
    except Exception as e:
        print("Erreur lors de la création des relations COLLAB_AVEC :", e)

    try:
        return run_query(driver, query_extract_top)
    except Exception as e:
        print("Erreur lors de l'extraction des collaborations :", e)
        return []
