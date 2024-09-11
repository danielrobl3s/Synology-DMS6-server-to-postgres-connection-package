import psycopg2
from psycopg2 import sql
import sys

def connect_to_nas_postgres():
    # Connection parameters
    params = {
        'host': 'YOUR_HOST_IP_ADDRESS',  # Replace with your NAS's IP address
        'port': 'YOUR_MAPPED_PORT_TO_POSTGRES',           # The mapped port for PostgreSQL
        'database': 'YOUR_DATABASE',      # The database name
        'user': 'YOUR_DB_USER',      # The database user
        'password': 'YOUR_DB_USER_PASSWORD'  # The database password
    }

    try:
        # Attempt to establish a connection
        conn = psycopg2.connect(**params)
        print("Successfully connected to the PostgreSQL database.")
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def execute_query(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL(query))
            if cur.description:
                rows = cur.fetchall()
                for row in rows:
                    print(row)
            else:
                print("Query executed successfully.")
            conn.commit()
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()
        

#Create operation
def insert_into_table(conn, table_name, column_names, values):
    """
    Inserts data into the specified table.
    
    :param conn: Database connection object
    :param table_name: Name of the table to insert into
    :param column_names: List of column names
    :param values: List of values to insert
    """
    columns = ', '.join(column_names)
    placeholders = ', '.join(['%s'] * len(values))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, values)
        conn.commit()
        print(f"Successfully inserted data into {table_name}")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        conn.rollback()



# Read operation
def select_from_table(conn, table_name, columns="*", condition=None):
    """
    Selects data from the specified table and prints the results.
    
    :param conn: Database connection object
    :param table_name: Name of the table to select from
    :param columns: Columns to select (default is all columns)
    :param condition: WHERE condition for the query (optional)
    :return: List of tuples containing the selected data
    """
    query = f"SELECT {columns} FROM {table_name}"
    if condition:
        query += f" WHERE {condition}"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        
        print(f"Successfully retrieved data from {table_name}")
        
        # Print column names
        if isinstance(columns, str) and columns == "*":
            col_names = [desc[0] for desc in cur.description]
        else:
            col_names = columns.split(", ") if isinstance(columns, str) else columns
        print("Columns:", ", ".join(col_names))
        
        # Print results
        if results:
            for row in results:
                print(row)
        else:
            print("No results found.")
        
        return results
    except Exception as e:
        print(f"Error retrieving data from {table_name}: {e}")
        return []
    


# Update operation
def update_table(conn, table_name, set_values, condition):
    """
    Updates data in the specified table.
    
    :param conn: Database connection object
    :param table_name: Name of the table to update
    :param set_values: Dictionary of column names and new values
    :param condition: WHERE condition for the update
    """
    set_clause = ', '.join([f"{key} = %s" for key in set_values.keys()])
    query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, list(set_values.values()))
        conn.commit()
        print(f"Successfully updated data in {table_name}")
    except Exception as e:
        print(f"Error updating data in {table_name}: {e}")
        conn.rollback()



# Delete operation
def delete_from_table(conn, table_name, condition):
    """
    Deletes data from the specified table.
    
    :param conn: Database connection object
    :param table_name: Name of the table to delete from
    :param condition: WHERE condition for the delete operation
    """
    query = f"DELETE FROM {table_name} WHERE {condition}"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
        print(f"Successfully deleted data from {table_name}")
    except Exception as e:
        print(f"Error deleting data from {table_name}: {e}")
        conn.rollback()



def execute_queries_from_file(conn, file_path):
    try:
        with open(file_path, 'r') as file:
            queries = file.read().split(';')
            for query in queries:
                query = query.strip()
                if query:
                    execute_query(conn, query)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading file: {e}")



def close_connection(conn):
    if conn:
        conn.close()
        print("Database connection closed.")



def main():
    conn = connect_to_nas_postgres()
    if not conn:
        return

    #YOUR CODE HERE

    close_connection(conn)

if __name__ == "__main__":
    main()
