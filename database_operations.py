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
            database="soraka",
            port=3306 
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

def insert_df_to_mysql(connection, table_name, df):
  """
  Inserts data from a pandas DataFrame into a MySQL table. 
  Drops and recreates the table before inserting data.

  Args:
      df: The pandas DataFrame containing the data to insert.
      table_name: The name of the MySQL table.
      connection: The established database connection object.
  """
  try:
      cursor = connection.cursor()

      # Drop the table if it exists
      drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
      cursor.execute(drop_table_query)

      # Create the table based on DataFrame columns
      create_table_query = f"CREATE TABLE {table_name} ("
      for col in df.columns:
          create_table_query += f"`{col}` {df[col].dtype}," 
      create_table_query = create_table_query[:-1] + ")"  # Remove trailing comma
      cursor.execute(create_table_query)

      # Insert data from DataFrame
      for index, row in df.iterrows():
          insert_query = f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES ({','.join(['%s'] * len(df.columns))})"
          cursor.execute(insert_query, tuple(row))

      connection.commit()
      print(f"Data inserted successfully into {table_name}")

  except mysql.connector.Error as error:
      print(f"Error inserting data: {error}")
  finally:
      if cursor:
          cursor.close()



