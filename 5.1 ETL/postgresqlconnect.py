import mysql.connector
from mysql.connector import Error

# Connection details
dsn_hostname = '172.21.145.117'
dsn_user = 'root'
dsn_pwd = 'Av6cgzUbzlMbEofvxLQSXyeQ'
dsn_port = '3306'
dsn_database = 'sales'  # Use the appropriate database

try:
    # Create connection
    conn = mysql.connector.connect(
        host=dsn_hostname,
        user=dsn_user,
        password=dsn_pwd,
        database=dsn_database,
        port=dsn_port
    )

    if conn.is_connected():
        print("Connected to MySQL database")

        # Create cursor object
        cursor = conn.cursor()

        # Create table
        SQL = """CREATE TABLE IF NOT EXISTS products (
                    rowid INT PRIMARY KEY NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    category VARCHAR(255) NOT NULL
                )"""
        cursor.execute(SQL)
        print("Table created")

        # Insert data
        cursor.execute("INSERT INTO products (rowid, product, category) VALUES (1, 'Television', 'Electronics')")
        cursor.execute("INSERT INTO products (rowid, product, category) VALUES (2, 'Laptop', 'Electronics')")
        cursor.execute("INSERT INTO products (rowid, product, category) VALUES (3, 'Mobile', 'Electronics')")

        # Commit the transaction
        conn.commit()

        # Insert list of records
        list_of_records = [(5, 'Tablet', 'Electronics'), (6, 'Headphones', 'Accessories')]
        
        for row in list_of_records:
            SQL = "INSERT INTO products (rowid, product, category) VALUES (%s, %s, %s)"
            cursor.execute(SQL, row)
        
        # Commit the transaction for the batch insert
        conn.commit()

        # Query data
        cursor.execute('SELECT * FROM products')
        rows = cursor.fetchall()

        # Print each row
        for row in rows:
            print(row)

except Error as e:
    print(f"Error: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Connection closed")