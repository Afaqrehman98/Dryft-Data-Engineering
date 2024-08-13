import logging

import psycopg2
from config import DB_NAME, USER_NAME, PASSWORD, HOST, PORT
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
db_params = {
    'dbname': DB_NAME,
    'user': USER_NAME,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT  # Default PostgreSQL port
}

# SQL scripts to drop and create the table
drop_table_query = 'DROP TABLE IF EXISTS reservations CASCADE;'

create_table_query = '''
CREATE TABLE reservations (
    id SERIAL PRIMARY KEY,
    pos INT,
    reserv_nr VARCHAR(50),
    bedarfsmenge DECIMAL,
    bme VARCHAR(10),
    data VARCHAR(50),
    bed_termin DATE,
    gel BOOLEAN,
    kda_pos INT,
    auftrag VARCHAR(50),
    angel_am DATE,
    uhrzeit TIME,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59'
);
'''


def execute_query(connection, query, query_description):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        logging.info(f"{query_description} executed successfully")
    except Exception as error:
        logging.error(f"Error executing {query_description}: %s", error)
        raise
    finally:
        cursor.close()


def main():
    connection = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        logging.info("Database connection established")

        # Drop the existing reservations table
        execute_query(connection, drop_table_query, "Drop table")

        # Create the reservations table
        execute_query(connection, create_table_query, "Create table")

    except Exception as error:
        logging.error("Failed to execute script: %s", error)
    finally:
        if connection:
            connection.close()
            logging.info("PostgreSQL connection is closed")


if __name__ == "__main__":
    main()
