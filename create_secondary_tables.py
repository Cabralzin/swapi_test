import psycopg2

# PostgreSQL credentials
dbname = "postgres"
user = "postgres"
password = "D@nKlarH@ands1995"
host = "localhost"

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    print("Connected to PostgreSQL successfully!")
except Exception as e:
    print("Unable to connect to the database:", e)

# Define table creation queries
table_queries = {
    'movie_characters': """
        CREATE SCHEMA IF NOT EXISTS trusted;
        DROP TABLE IF EXISTS trusted.movie_characters;
        CREATE TABLE trusted.movie_characters (
            id INT,
            title TEXT,
            episode_id INT,
            opening_crawl TEXT,
            director TEXT,
            producer TEXT,
            release_date DATE,
            characters TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'movie_planets': """
        CREATE TABLE trusted.movie_planets (
            id INT,
            title TEXT,
            episode_id INT,
            opening_crawl TEXT,
            director TEXT,
            producer TEXT,
            release_date DATE,
            planets TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'movie_starships': """
        CREATE TABLE trusted.movie_starships (
            id INT,
            title TEXT,
            episode_id INT,
            opening_crawl TEXT,
            director TEXT,
            producer TEXT,
            release_date DATE,
            starships TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'movie_vehicles': """
        CREATE TABLE trusted.movie_vehicles (
            id INT,
            title TEXT,
            episode_id INT,
            opening_crawl TEXT,
            director TEXT,
            producer TEXT,
            release_date DATE,
            vehicles TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'movie_species': """
        CREATE TABLE trusted.movie_species (
            id INT,
            title TEXT,
            episode_id INT,
            opening_crawl TEXT,
            director TEXT,
            producer TEXT,
            release_date DATE,
            species TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
        
    """,
    'people': """
        CREATE TABLE trusted.people (
            id INT,
            name TEXT,
            height INT,
            mass FLOAT,
            hair_color TEXT,
            skin_color TEXT,
            eye_color TEXT,
            birth_year TEXT,
            gender TEXT,
            homeworld TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
        'people_films': """
        CREATE TABLE trusted.people_films (
            id INT,
            films TEXT,
            url TEXT
        );
    """,
    'people_species': """
        CREATE TABLE trusted.people_species (
            id INT,
            species TEXT,
            url TEXT
        );
    """,
    'people_vehicles': """
        CREATE TABLE trusted.people_vehicles (
            id INT,
            vehicles TEXT,
            url TEXT
        );
    """,
    'people_starships': """
        CREATE TABLE trusted.people_starships (
            id INT,
            starships TEXT,
            url TEXT
        );
    """,
    'planets': """
        CREATE TABLE trusted.planets (
            id INT,
            name TEXT,
            rotation_period TEXT,
            orbital_period TEXT,
            diameter TEXT,
            climate TEXT,
            gravity TEXT,
            terrain TEXT,
            surface_water TEXT,
            population TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'planet_residents': """
        CREATE TABLE trusted.planet_residents (
            id INT,
            residents TEXT,
            url TEXT
        );
    """,
    'planet_films': """
        CREATE TABLE trusted.planet_films (
            id INT,
            films TEXT,
            url TEXT
        );
    """,
    'species': """
        CREATE TABLE trusted.species (
            id INT,
            name TEXT,
            classification TEXT,
            designation TEXT,
            average_height TEXT,
            skin_colors TEXT,
            hair_colors TEXT,
            eye_colors TEXT,
            average_lifespan TEXT,
            homeworld TEXT,
            language TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'species_people': """
        CREATE TABLE trusted.species_people (
            id INT,
            people TEXT,
            url TEXT
        );
    """,
    'species_films': """
        CREATE TABLE trusted.species_films (
            id INT,
            films TEXT,
            url TEXT
        );
    """,
        'starships': """
        CREATE TABLE trusted.starships (
            id INT,
            name TEXT,
            model TEXT,
            manufacturer TEXT,
            cost_in_credits TEXT,
            length TEXT,
            max_atmosphering_speed TEXT,
            crew TEXT,
            passengers TEXT,
            cargo_capacity TEXT,
            consumables TEXT,
            hyperdrive_rating TEXT,
            mglt TEXT,
            starship_class TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'starship_pilots': """
        CREATE TABLE trusted.starship_pilots (
            id INT,
            pilots TEXT,
            url TEXT
        );
    """,
    'starship_films': """
        CREATE TABLE trusted.starship_films (
            id INT,
            films TEXT,
            url TEXT
        );
    """,
    'vehicles': """
        CREATE TABLE trusted.vehicles (
            id INT,
            name TEXT,
            model TEXT,
            manufacturer TEXT,
            cost_in_credits TEXT,
            length TEXT,
            max_atmosphering_speed TEXT,
            crew TEXT,
            passengers TEXT,
            cargo_capacity TEXT,
            consumables TEXT,
            vehicle_class TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            url TEXT
        );
    """,
    'vehicle_pilots': """
        CREATE TABLE trusted.vehicle_pilots (
            id INT,
            pilots TEXT,
            url TEXT
        );
    """,
    'vehicle_films': """
        CREATE TABLE trusted.vehicle_films (
            id INT,
            films TEXT,
            url TEXT
        );
    """
}

# Create tables
try:
    cursor = conn.cursor()
    for table, query in table_queries.items():
        cursor.execute(query)
    conn.commit()
    print("Tables created successfully!")
    cursor.close()
except Exception as e:
    print("Error creating tables:", e)
finally:
    if conn is not None:
        conn.close()  # Close the database connection
