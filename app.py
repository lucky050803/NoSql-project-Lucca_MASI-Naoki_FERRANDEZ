from connexion import connect_mongo, connect_neo4j
import queries as q
from pprint import pprint

def main():
    # Connexion aux bases de données
    mongo_db = connect_mongo()
    neo4j_driver = connect_neo4j()

    # Récupération de la collection de films
    films = mongo_db["films"]

    print("\n--- Requêtes MongoDB ---")
    pprint(list(q.most_common_year(films)))
    print("Nombre de films après 1999:", q.count_movies_after_1999(films))
    pprint(list(q.average_votes_2007(films)))
    pprint(list(q.movies_per_year(films)))
    print("Genres disponibles:", q.available_genres(films))
    pprint(list(q.highest_revenue_movie(films)))
    pprint(list(q.prolific_directors(films)))
    pprint(list(q.most_profitable_genre(films)))
    pprint(list(q.top_3_movies_per_decade(films)))
    pprint(list(q.longest_movie_by_genre(films)))
    pprint(list(q.filtered_view(films)))
    pprint(q.runtime_vs_revenue_data(films))
    pprint(list(q.average_runtime_by_decade(films)))

    print("\n--- Requêtes Neo4j ---")
    pprint(q.most_featured_actor(neo4j_driver))
    pprint(q.coactors_with_anne_hathaway(neo4j_driver))
    pprint(q.highest_grossing_actor(neo4j_driver))
    pprint(q.average_votes(neo4j_driver))
    pprint(q.most_common_genre(neo4j_driver))
    pprint(q.movies_with_group_members(neo4j_driver, ["Robert Downey Jr.", "Scarlett Johansson"]))
    pprint(q.director_with_most_unique_actors(neo4j_driver))
    pprint(q.most_connected_films(neo4j_driver))
    pprint(q.actors_with_most_directors(neo4j_driver))
    pprint(q.recommend_by_genre(neo4j_driver, "Leonardo DiCaprio"))
    pprint(q.influence_between_directors(neo4j_driver))
    pprint(q.shortest_path_between_actors(neo4j_driver, "Brad Pitt", "Matt Damon"))
    pprint(q.actor_communities(neo4j_driver))

    print("\n--- Requêtes croisées MongoDB & Neo4j ---")
    pprint(q.similar_genre_diff_director(neo4j_driver))
    pprint(q.recommend_to_user_based_on_actor(neo4j_driver, "Tom Hanks"))
    pprint(q.create_competition_relationships(neo4j_driver))
    pprint(q.frequent_collabs_and_success(neo4j_driver))

    # Fermeture de la connexion Neo4j
    neo4j_driver.close()

if __name__ == "__main__":
    main()
