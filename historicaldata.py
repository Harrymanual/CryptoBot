import csv
import psycopg2
import pandas as pd

from sqlalchemy import create_engine, text



def write_historical_data_(csv_file_path):

    db, conn = connection('config.txt')

    print("starting")

    # Create the 'historicaldata' table if it does not exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS historicaldata (
            Date DATE,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Adj_Close FLOAT,
            Volume BIGINT
        );
    ''')
    conn.commit()
    print("Starting 1")

    # Open the CSV file
    with open(csv_file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        # Skip the header row (since your CSV has headers)
        next(reader)

        # Loop through the rows in the CSV
        for row in reader:
            # Convert Date to the correct format and insert data into table
            conn.execute('''
                INSERT INTO historicaldata (Date, Open, High, Low, Close, Adj_Close, Volume)
                VALUES (TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s, %s, %s, %s)
            ''', row)

    # Commit the transaction
    conn.commit()
    # Close the cursor and the connection
    conn.close()
    db.dispose()
    print("Data uploaded successfully.")


def connection(config_file):
    with open(config_file, 'r') as file:
        db_host = file.readline().strip()
        db_name = file.readline().strip()
        db_user = file.readline().strip()
        db_password = file.readline().strip()

    # Connection string using the credentials from the config file
    conn_string = f'{db_user}:{db_password}@{db_host}/{db_name}'
    db = create_engine(f'postgresql+psycopg2://{conn_string}', echo=False)

    # Connect to your PostgreSQL database
    conn = db.connect()

    return db, conn


def main():
    db, conn = connection('config.txt')
    query = 'SELECT volume FROM historicaldata'
    df = pd.read_sql_query(query, db)
    print(df.head())

    conn.close()
    db.dispose()


if __name__ == '__main__':
    main()
