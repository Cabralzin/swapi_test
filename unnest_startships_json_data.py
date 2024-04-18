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
sql_query = "SELECT * FROM raw.starships"

# Fetch data and create DataFrame
try:
    df = pd.read_sql_query(sql_query, conn)
    print("DataFrame created successfully!")

    # Replace 'unknown' or 'n/a' values with None or 0
    df.replace(['unknown', 'n/a'], [None, 0], inplace=True)

    # Convert numeric columns to appropriate data types
    numeric_columns = ['length', 'max_atmosphering_speed', 'crew', 'passengers', 'cargo_capacity', 'hyperdrive_rating', 'mglt']
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    # Define columns for each table
    table_columns = {
        'starships': ['id', 'name', 'model', 'manufacturer', 'cost_in_credits', 'length', 'max_atmosphering_speed', 'crew', 'passengers', 'cargo_capacity', 'consumables', 'hyperdrive_rating', 'mglt', 'starship_class', 'created', 'edited', 'url'],
        'starship_films': ['id', 'films', 'url'],
        'starship_pilots': ['id', 'pilots', 'url']
    }

    # Iterate through tables and corresponding DataFrames
    for table, columns in table_columns.items():
        # Select columns for the table
        df_selected = df[columns]

        # Explode the specified columns
        if 'films' in columns:
            df_selected['films'] = df_selected['films'].str.strip('{}').str.split(',')
            df_selected = df_selected.explode('films')
        if 'pilots' in columns:
            df_selected['pilots'] = df_selected['pilots'].str.strip('{}').str.split(',')
            df_selected = df_selected.explode('pilots')

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
