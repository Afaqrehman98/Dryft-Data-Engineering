import logging

import pandas as pd
import psycopg2
from dateutil import parser

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
daily_updates_file = 'data/reservation_update.csv'


def process_daily_updates(file_path, connection):
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

        # Remove thousands separator and replace comma with dot for decimal conversion
        df['bedarfsmenge'] = df['bedarfsmenge'].str.replace('.', '').str.replace(',', '.').str.strip().astype(float)

        # Convert 'gel' to boolean, assuming 0 = False, 1 = True, and handle NaN as False
        df['gel'] = df['gel'].apply(lambda x: True if x == 'X' else False)

        # Use a custom function to parse dates
        df['bed_termin'] = df['bed_termin'].apply(lambda x: parser.parse(x, dayfirst=True).date())
        df['angel_am'] = df['angel_am'].apply(lambda x: parser.parse(x, dayfirst=True).date())

        # Explicitly define the time format for 'uhrzeit'
        df['uhrzeit'] = pd.to_datetime(df['uhrzeit'], format='%H:%M:%S').dt.time

        with connection.cursor() as cursor:
            for index, row in df.iterrows():
                # Update the existing records
                update_query = '''
                UPDATE reservations
                SET bedarfsmenge = %s, bme = %s, material = %s,
                    bed_termin = %s, gel = %s, kda_pos = %s,
                    auftrag = %s, angel_am = %s, uhrzeit = %s
                WHERE reserv_nr = %s AND pos = %s;
                '''
                cursor.execute(update_query, (
                    row['bedarfsmenge'], row['bme'], row['material'],
                    row['bed_termin'], row['gel'], row['kda_pos'],
                    row['auftrag'], row['angel_am'], row['uhrzeit'],
                    str(row['reserv_nr']), str(row['pos'])  # Convert to string for comparison
                ))

                # Insert new records if they do not exist
                insert_query = '''
                INSERT INTO reservations (
                    pos, reserv_nr, bedarfsmenge, bme, material,
                    bed_termin, gel, kda_pos, auftrag, angel_am, uhrzeit
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM reservations WHERE reserv_nr = %s AND pos = %s
                );
                '''
                cursor.execute(insert_query, (
                    row['pos'], row['reserv_nr'], row['bedarfsmenge'], row['bme'], row['material'],
                    row['bed_termin'], row['gel'], row['kda_pos'], row['auftrag'], row['angel_am'], row['uhrzeit'],
                    str(row['reserv_nr']), str(row['pos'])  # Convert to string for comparison
                ))

            connection.commit()
        logging.info("Daily updates processed successfully")
    except Exception as error:
        logging.error("Error processing daily updates: %s", error)
        connection.rollback()
        raise


def main():
    connection = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        logging.info("Database connection established")

        # Process the daily updates
        process_daily_updates(daily_updates_file, connection)
    except Exception as error:
        logging.error("Failed to execute script: %s", error)
    finally:
        if connection:
            connection.close()
            logging.info("PostgreSQL connection is closed")


if __name__ == "__main__":
    main()
