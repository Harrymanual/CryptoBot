import requests
import time
import threading
from sqlalchemy import create_engine, text

# Database connection function
def connection(config_file):
    with open(config_file, 'r') as file:
        db_host = file.readline().strip()
        db_name = file.readline().strip()
        db_user = file.readline().strip()
        db_password = file.readline().strip()
        db_port = file.readline().strip()

    conn_string = f'{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    try:
        db = create_engine(f'postgresql+psycopg2://{conn_string}', echo=False)
        conn = db.connect()
        print("Connected successfully.")
    except Exception as e:
        print('Connection to database failed:', e)
        db, conn = None, None

    return db, conn

# Function to create the 'ourdata' table
def create_ourdata_table(conn):
    conn.execute(text('''
        CREATE TABLE IF NOT EXISTS ourdata (
            timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            btc_price_usd FLOAT
        );
    '''))
    conn.commit()

# Function to write the BTC price into the 'ourdata' table
def write_btc_price(conn, price):
    conn.execute(text('''
        INSERT INTO ourdata (btc_price_usd)
        VALUES (:price)
    '''), {'price': price})
    conn.commit()

# Function to fetch the BTC price
def get_btc_price():
    response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd')
    data = response.json()
    return data['bitcoin']['usd']

# Main function
def main(stop_event):
    db, conn = connection('config.txt')  # Ensure 'config.txt' is in the same directory as this script
    create_ourdata_table(conn)

    while not stop_event.is_set():
        price = get_btc_price()
        print(f"The current price of BTC is: ${price} USD")
        write_btc_price(conn, price)
        time.sleep(10)

    # Close the connection when the loop is terminated
    conn.close()
    db.dispose()
    print("Disconnected from database and exiting.")

# Thread for the main function
stop_event = threading.Event()
worker_thread = threading.Thread(target=main, args=(stop_event,))

# Start the worker thread
worker_thread.start()

# Wait for user input to stop the loop
while True:
    user_input = input()
    if user_input.lower() == 'end':
        print("Ending the BTC price fetching process...")
        stop_event.set()
        break

# Wait for the worker thread to finish before exiting the script
worker_thread.join()
print("Script terminated.")
