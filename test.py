import psycopg2


# Read database credentials from the config file
with open('config.txt', 'r') as file:
    db_host = file.readline().strip()
    db_name = file.readline().strip()
    db_user = file.readline().strip()
    db_password = file.readline().strip()

# Connection string using the credentials from the config file
conn_string = f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_password}' sslmode='require'"

# Connect to your PostgreSQL database
conn = psycopg2.connect(conn_string)

# Create a cursor object
cur = conn.cursor()

# Let's perform a basic operation: creating a new table
cur.execute('''
            CREATE TABLE IF NOT EXISTS demo_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value INTEGER
            );
            ''')

# Commit the transaction to make sure the table is created
conn.commit()

# Let's insert a new row into the table
cur.execute('''
            INSERT INTO demo_table (name, value)
            VALUES (%s, %s)
            RETURNING id;
            ''', ('Sample Name', 222))

# Fetch the id of the newly inserted row and print it
new_id = cur.fetchone()[0]
print(f"Inserted new row with id {new_id}")

# Commit the insert transaction
conn.commit()

# Now, let's select data from the table and print it
cur.execute('SELECT * FROM demo_table')
records = cur.fetchall()

for record in records:
    print(record)

# Close the cursor and the connection
cur.close()
conn.close()
