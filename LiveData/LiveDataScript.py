import requests
import time
import threading
from sqlalchemy import create_engine, text
from lxml import html

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

# Function to create the 'ourdata' table with an additional column for Fear and Greed Index
def create_ourdata_table(conn):
    conn.execute(text('''
        CREATE TABLE IF NOT EXISTS greedprice (
            timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            btc_price_usd FLOAT,
            fear_greed_index TEXT
        );
    '''))
    conn.commit()

# Function to write the BTC price and Fear and Greed Index into the 'ourdata' table
def write_data(conn, price, index):
    conn.execute(text('''
        INSERT INTO greedprice (btc_price_usd, fear_greed_index)
        VALUES (:price, :index)
    '''), {'price': price, 'index': index})
    conn.commit()

# Function to fetch the BTC price
def get_btc_price():
    response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd')
    data = response.json()
    return data['bitcoin']['usd']

# Function to fetch the Fear and Greed Index
def get_fear_greed_index():
    response = requests.get('https://alternative.me/crypto/fear-and-greed-index/')
    tree = html.fromstring(response.content)
    index = tree.xpath('/html/body/div/main/section/div/div[3]/div[2]/div/div/div[1]/div[2]/div/text()')
    return index[0].strip() if index else "Not Found"

# Main function
def main(stop_event):
    db, conn = connection('config.txt')  # Ensure 'config.txt' is in the same directory as this script
    create_ourdata_table(conn)

    while not stop_event.is_set():
        price = get_btc_price()
        index = get_fear_greed_index()
        print(f"The current price of BTC is: ${price} USD, Fear and Greed Index: {index}")
        write_data(conn, price, index)
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
