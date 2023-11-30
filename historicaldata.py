import csv
import psycopg2
import pandas as pd
# PLEASE DONT RUN THIS AGAIN OR ILL BE SAD :( !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Read database credentials from the config file
with open('config.txt', 'r') as file:
    db_host = file.readline().strip()
    db_name = file.readline().strip()
    db_user = file.readline().strip()
    db_password = file.readline().strip()
# CSV file path
csv_file_path = 'BTC-USD.csv'
# Connection string using the credentials from the config file
conn_string = f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_password}' sslmode='require'"
# Connect to your PostgreSQL database
conn = psycopg2.connect(conn_string)
cur = conn.cursor()
print("starting")
# Create the 'historicaldata' table if it does not exist
cur.execute('''
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
print("starting 1")
# Open the CSV file
with open(csv_file_path, 'r') as csvfile:
    reader = csv.reader(csvfile)

    # Skip the header row (since your CSV has headers)
    next(reader)

    # Loop through the rows in the CSV
    for row in reader:
        # Convert Date to the correct format and insert data into table
        cur.execute('''
            INSERT INTO historicaldata (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES (TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s, %s, %s, %s)
        ''', row)
# Commit the transaction
conn.commit()
# Close the cursor and the connection
cur.close()
conn.close()
print("Data uploaded successfully.")