import mysql.connector
import psycopg2  # or import ibm_db for DB2

# MySQL connection parameters
MYSQL_HOST = '172.21.145.117'
MYSQL_PORT = 3306
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'Av6cgzUbzlMbEofvxLQSXyeQ'
MYSQL_DATABASE = 'sales'

# PostgreSQL connection parameters (fill these in with actual values)
PG_HOST = 'your_pg_host'
PG_PORT = 'your_pg_port'
PG_USERNAME = 'your_pg_username'
PG_PASSWORD = 'your_pg_password'
PG_DATABASE = 'your_pg_database'

def get_last_rowid():
    """Get the last rowid from the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USERNAME,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(rowid) FROM sales_data;")
        last_rowid = cursor.fetchone()[0]
        cursor.close()
        return last_rowid
    except Exception as e:
        print(f"Error fetching last rowid: {e}")
    finally:
        if conn:
            conn.close()

def get_latest_records(rowid):
    """Get records from MySQL with rowid greater than the specified rowid."""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USERNAME,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sales_data WHERE rowid > %s;", (rowid,))
        records = cursor.fetchall()
        cursor.close()
        return records
    except Exception as e:
        print(f"Error fetching new records: {e}")
    finally:
        if conn:
            conn.close()

def insert_records(records):
    """Insert records into PostgreSQL data warehouse."""
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USERNAME,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cursor = conn.cursor()
        for record in records:
            cursor.execute("""
                INSERT INTO sales_data (column1, column2, column3) VALUES (%s, %s, %s);
            """, (record['column1'], record['column2'], record['column3']))  # Adjust columns as needed
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error inserting records: {e}")
    finally:
        if conn:
            conn.close()

# Main execution
if __name__ == "__main__":
    last_row_id = get_last_rowid()
    print("Last row id on production data warehouse =", last_row_id)

    new_records = get_latest_records(last_row_id)
    print("New rows on staging data warehouse =", len(new_records))

    insert_records(new_records)
    print("New rows inserted into production data warehouse =", len(new_records))