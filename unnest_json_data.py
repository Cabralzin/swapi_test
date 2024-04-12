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
    df = df.explode('characters').explode('planets').explode('starships').explode('vehicles').explode('species')

    # Group by specified columns
    grouped_df = df.groupby(['id', 'title', 'episode_id', 'opening_crawl', 'director', 'producer', 'release_date', 'characters', 'planets', 'starships', 'vehicles', 'species', 'created', 'edited', 'url']).size().reset_index(name='count')

    # Insert grouped DataFrame into a table in the database
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS trusted")
    cursor.execute("DROP TABLE IF EXISTS trusted.films")
    cursor.execute("CREATE TABLE trusted.films (id INT, title TEXT, episode_id INT, opening_crawl TEXT, director TEXT, producer TEXT, release_date DATE, characters TEXT, planets TEXT, starships TEXT, vehicles TEXT, species TEXT, created TEXT, edited TEXT, url TEXT, count INT)")
    cursor.close()

    grouped_df_columns = grouped_df.columns.tolist()
    grouped_df_columns_str = ','.join(grouped_df_columns)
    grouped_df_values = ','.join(['%s'] * len(grouped_df_columns))
    insert_query = f"INSERT INTO trusted.films ({grouped_df_columns_str}) VALUES ({grouped_df_values})"

    cursor = conn.cursor()
    cursor.executemany(insert_query, grouped_df.values.tolist())
    conn.commit()
    cursor.close()

    print("Grouped DataFrame inserted into database table 'trusted.films'")
except Exception as e:
    print("Error fetching data and creating DataFrame:", e)
finally:
    if conn is not None:
        conn.close()  # Close the database connection
