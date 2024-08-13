import logging

import pandas as pd
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

# CSV file path
base_data_file = 'data/reservation_base.csv'


def load_initial_data(file_path, connection):
    try:
        # Load the CSV into a DataFrame
        df = pd.read_csv(file_path)

        # Drop the unwanted column 'BME.1'
        df = df.drop('BME.1', axis=1)

        # Rename columns to match the table schema
        df.columns = [
            'pos', 'reserv_nr', 'bedarfsmenge', 'bme', 'material',
            'bed_termin', 'gel', 'kda_pos', 'auftrag', 'angel_am', 'uhrzeit'
        ]

        # Clean the 'bedarfsmenge' column
        df['bedarfsmenge'] = df['bedarfsmenge'].str.replace(',', '.').str.strip().astype(float)

        df['gel'] = df['gel'].apply(lambda x: True if x == 'X' else False)

        # Parse 'bed_termin' and 'angel_am' with the correct date format: dayfirst=True
        df['bed_termin'] = pd.to_datetime(df['bed_termin'], format='%d.%m.%y', dayfirst=True).dt.date
        df['angel_am'] = pd.to_datetime(df['angel_am'], format='%d.%m.%y', dayfirst=True).dt.date

        # Explicitly define the time format for 'uhrzeit'
        df['uhrzeit'] = pd.to_datetime(df['uhrzeit'], format='%H:%M:%S').dt.time

        # Insert into the database
        with connection.cursor() as cursor:
            for index, row in df.iterrows():
                insert_query = '''
                INSERT INTO reservations (
                    pos, reserv_nr, bedarfsmenge, bme, material,
                    bed_termin, gel, kda_pos, auftrag, angel_am, uhrzeit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                '''
                cursor.execute(insert_query, tuple(row))
            connection.commit()
        logging.info("Initial data loaded successfully")
    except Exception as error:
        logging.error("Error loading initial data: %s", error)
        connection.rollback()
        raise


def main():
    connection = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        logging.info("Database connection established")

        # Load the initial base data
        load_initial_data(base_data_file, connection)
    except Exception as error:
        logging.error("Failed to execute script: %s", error)
    finally:
        if connection:
            connection.close()
            logging.info("PostgreSQL connection is closed")


if __name__ == "__main__":
    main()
