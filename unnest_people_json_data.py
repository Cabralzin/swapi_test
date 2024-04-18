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
sql_query = "SELECT * FROM raw.people"

# Fetch data and create DataFrame
try:
    df = pd.read_sql_query(sql_query, conn)
    print("DataFrame created successfully!")

    # Replace 'unknown' values in specified columns with None
    invalid_columns = ['height', 'mass', 'skin_color', 'eye_color', 'birth_year']
    for column in invalid_columns:
        df[column] = df[column].replace('unknown', None)

    # Remove commas from 'mass' column values
    df['mass'] = df['mass'].str.replace(',', '')

    # Define columns for each table
    table_columns = {
        'people': ['id', 'name', 'height', 'mass', 'hair_color', 'skin_color', 'eye_color', 'birth_year', 'gender', 'homeworld', 'created', 'edited', 'url'],
        'people_films': ['id', 'films', 'url'],
        'people_species': ['id', 'species', 'url'],
        'people_vehicles': ['id', 'vehicles', 'url'],
        'people_starships': ['id', 'starships', 'url']
    }

    # Iterate through tables and corresponding DataFrames
    for table, columns in table_columns.items():
        # Select columns for the table
        df_selected = df[columns]

        # Explode the specified columns
        if 'films' in columns:
            df_selected['films'] = df_selected['films'].str.strip('{}').str.split(',')
            df_selected = df_selected.explode('films')
        if 'species' in columns:
            df_selected['species'] = df_selected['species'].str.strip('{}').str.split(',')
            df_selected = df_selected.explode('species')
        if 'vehicles' in columns:
            df_selected['vehicles'] = df_selected['vehicles'].str.strip('{}').str.split(',')
            df_selected = df_selected.explode('vehicles')
        if 'starships' in columns:
            df_selected['starships'] = df_selected['starships'].str.strip('{}').str.split(',')
            df_selected = df_selected.explode('starships')

        # Insert data into the table
        cursor = conn.cursor()
        for index, row in df_selected.iterrows():
            insert_query = f"INSERT INTO trusted.{table} VALUES ({', '.join(['%s'] * len(columns))})"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        cursor.close()

        print(f"{table.capitalize()} DataFrame inserted into database table 'trusted.{table}'")

except Exception as e:
    print("Error fetching data and creating DataFrames:", e)
finally:
    if conn is not None:
        conn.close()  # Close the database connection
