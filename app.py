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
    print("1:")
    #pprint(list(q.most_common_year(films)))

    print("2:")
    #print("Nombre de films après 1999:", q.count_movies_after_1999(films))

    print("3:")
    #pprint(list(q.average_votes_2007(films)))

    print("4:")
    #pprint(list(q.movies_per_year(films)))

    print("5:")
    #print("Genres disponibles:", q.available_genres(films))

    print("6:")
    #pprint(list(q.highest_revenue_movie(films)))

    print("7:")
    #pprint(list(q.prolific_directors(films)))

    print("8:")
    #pprint(list(q.most_profitable_genre(films)))

    print("9:")
    #pprint(list(q.top_3_movies_per_decade(films)))

    print("10:")
    #pprint(list(q.longest_movie_by_genre(films)))

    print("11:")
    #pprint(list(q.filtered_view(films)))

    print("12:")
    #pprint(q.runtime_vs_revenue_data(films))  # returns list of dicts

    print("13:")
    #pprint(list(q.average_runtime_by_decade(films)))


    print("\n--- Requêtes Neo4j ---")
    print("14:")
    #pprint(q.most_featured_actor(neo4j_driver))

    print("15:")
    #pprint(q.coactors_with_anne_hathaway(neo4j_driver))

    print("16:")
    #pprint(q.highest_grossing_actor(neo4j_driver))

    print("17:")
    #pprint(q.average_votes(neo4j_driver))

    print("18:")
    #pprint(q.most_common_genre(neo4j_driver))

    print("19:")
    #pprint(q.movies_with_group_members(neo4j_driver, ["Robert Downey Jr.", "Scarlett Johansson"]))

    print("20:")
    #pprint(q.director_with_most_unique_actors(neo4j_driver))

    print("21:")
    #pprint(q.most_connected_films(neo4j_driver))

    print("22:")
    #pprint(q.actors_with_most_directors(neo4j_driver))

    print("23:")
    #pprint(q.recommend_by_genre(neo4j_driver, "Leonardo DiCaprio"))

    print("24:")
    #pprint(q.influence_between_directors(neo4j_driver))

    print("25:")
    #pprint(q.shortest_path_between_actors(neo4j_driver, "Leonardo DiCaprio", "Matt Damon"))

    print("26:")
    #pprint(q.actor_communities(neo4j_driver))


    print("\n--- Requêtes croisées MongoDB & Neo4j ---")
    print("27:")
    #pprint(q.similar_genre_diff_director(neo4j_driver))

    print("28:")
    #pprint(q.recommend_to_user_based_on_actor(neo4j_driver, "Tom Hanks"))

    print("29:")
    #pprint(q.create_competition_relationships(neo4j_driver))

    print("30:")
    pprint(q.frequent_collabs_and_success(neo4j_driver))

    # Fermeture de la connexion Neo4j
    neo4j_driver.close()

if __name__ == "__main__":
    main()
