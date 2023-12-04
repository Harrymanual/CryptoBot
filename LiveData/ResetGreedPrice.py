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

# Function to clear the 'ourdata' table
def clear_ourdata_table(conn):
    try:
        conn.execute(text('TRUNCATE TABLE greedprice;'))
        conn.commit()
        print("Table 'greedprice' has been cleared.")
    except Exception as e:
        print('Error clearing the table:', e)

def main():
    db, conn = connection('config.txt')  # Ensure 'config.txt' is in the same directory as this script
    clear_ourdata_table(conn)
    
    # Close the connection
    conn.close()
    db.dispose()
    print("Disconnected from database.")

if __name__ == '__main__':
    main()
