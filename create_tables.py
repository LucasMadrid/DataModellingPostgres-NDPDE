import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
     Description: This function is responsible of creating the connection to the database and creating the cursor to
     drop the database if already exists and then create it again.
     
     Arguments:
         None.
         
     Returns:
         cur: the cursor object.
         conn: connection to the database.
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
     Description: this function is responsible to drop the tables from sparkifydb database.
     
     Arguments:
         cur: the cursor object.
         conn: connection to the database.
         
     Returns:
         None.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
     Description: This function is responsible to create the tables of sparkifydb database.
     
     Arguments:
         cur: the cursor object.
         conn: connection to the database.
         
     Returns:
         None.
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()