import mysql.connector

def create_connection():
    """
    Establishes a connection to the MySQL database.

    Returns:
        mysql.connector.connect: A connection object.
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",  
            user="root",
            password="tQCjrTo-V!ftQnjxC9Yb",
            database="soraka"
        )
        print("Database connection successful.")
        return connection
    except mysql.connector.Error as error:
        print(f"Error connecting to MySQL: {error}")
        return None
    
def execute_query(connection, query):
    """
    Executes a given SQL query.

    Args:
        connection: The database connection object.
        query: The SQL query string.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully.")
    except mysql.connector.Error as error:
        print(f"Error executing query: {error}")

def fetch_all_rows(connection, query):
    """
    Fetches all rows from the result of a SELECT query.

    Args:
        connection: The database connection object.
        query: The SELECT query string.

    Returns:
        list: A list of tuples, where each tuple represents a row.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except mysql.connector.Error as error:
        print(f"Error fetching rows: {error}")
        return []

def insert_data(connection, query, data):
    """
    Inserts data into the database using an INSERT query.

    Args:
        connection: The database connection object.
        query: The INSERT query string (with placeholders).
        data: A tuple or list containing the data to be inserted.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        print("Data inserted successfully.")
    except mysql.connector.Error as error:
        print(f"Error inserting data: {error}")

def insert_df_to_mysql(df, table_name):
    """
    Inserts data from a pandas DataFrame into a MySQL table.

    Args:
        df: The pandas DataFrame containing the data to insert.
        table_name: The name of the MySQL table.
        host: The hostname of the MySQL server.
        user: The MySQL username.
        password: The MySQL password.
        database: The name of the MySQL database.
    """

    try:
        # Connect to the MySQL database
        cnx = mysql.connector.connect(
            host="localhost",  
            user="root",
            password="tQCjrTo-V!ftQnjxC9Yb",
            database="soraka"
        )

        # Create a cursor object
        cursor = cnx.cursor()

        # Get column names from the DataFrame
        cols = ','.join(df.columns)

        # Prepare the SQL INSERT statement with placeholders
        sql = "INSERT INTO {} ({}) VALUES (%s," * (len(df.columns) - 1) + "%s)".format(table_name, cols)

        # Insert data in batches for efficiency
        rows = df.to_numpy().tolist()
        for i in range(0, len(rows), 1000):  # Insert in batches of 1000 rows
            cursor.executemany(sql, rows[i:i+1000])

        # Commit the changes
        cnx.commit()

        print(f"Successfully inserted {len(df)} rows into {table_name}")

    except mysql.connector.Error as error:
        print(f"Error inserting data: {error}")
    finally:
        # Close the connection
        if cnx:
            cursor.close()
            cnx.close()

