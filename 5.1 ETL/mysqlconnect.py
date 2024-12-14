import mysql.connector

def connect_to_mysql():
    connection = None  # Initialize connection variable
    try:
        connection = mysql.connector.connect(
            host='172.21.145.117',  # Use the correct host
            user='root',             # Your MySQL username
            password='Av6cgzUbzlMbEofvxLQSXyeQ',  # Your MySQL password
            port=3306,               # Port number
            database='sales'         # Use the database if needed
        )
        if connection.is_connected():
            print("Connected to MySQL database")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    connect_to_mysql()