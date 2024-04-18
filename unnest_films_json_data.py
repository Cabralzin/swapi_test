import psycopg2
import pandas as pd

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

# SQL query to fetch data from the table
sql_query = "SELECT * FROM raw.films"

# Fetch data and create DataFrame
try:
    df = pd.read_sql_query(sql_query, conn)
    print("DataFrame created successfully!")

    # Remove curly braces and split by commas to create individual rows
    for column in ['characters', 'planets', 'starships', 'vehicles', 'species']:
        df[column] = df[column].str.strip('{}').str.split(',')

    # Explode the lists into separate rows
    df_characters = df.explode('characters')
    df_planets = df.explode('planets')
    df_starships = df.explode('starships')
    df_vehicles = df.explode('vehicles')
    df_species = df.explode('species')

    # Define table names and corresponding DataFrames
    tables = ['characters', 'planets', 'starships', 'vehicles', 'species']
    dataframes = [df_characters, df_planets, df_starships, df_vehicles, df_species]

    # Define columns for each table
    table_columns = {
        'characters': ['id', 'title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'characters', 'created', 'edited', 'url'],
        'planets': ['id', 'title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'planets', 'created', 'edited', 'url'],
        'starships': ['id', 'title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'starships', 'created', 'edited', 'url'],
        'vehicles': ['id', 'title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'vehicles', 'created', 'edited', 'url'],
        'species': ['id', 'title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'species', 'created', 'edited', 'url']
    }

    # Iterate through tables and corresponding DataFrames
    for table, df_table in zip(tables, dataframes):
        # Select columns for the table
        columns = table_columns[table]
        df_selected = df_table[columns]

        # Insert data into the table
        cursor = conn.cursor()
        for index, row in df_selected.iterrows():
            insert_query = f"INSERT INTO trusted.movie_{table} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        cursor.close()

        print(f"{table.capitalize()} DataFrame inserted into database table 'trusted.movie_{table}'")

except Exception as e:
    print("Error fetching data and creating DataFrames:", e)
finally:
    if conn is not None:
        conn.close()  # Close the database connection
