import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd


def connect_and_fetch_info():
    connection_url = sal.engine.URL.create(
        drivername="mssql+pyodbc",
        username="sa",  # SQL Server username
        password="Let_me_cook_now",  # SA password you used in Docker run
        host="localhost",  # Connecting to Docker on localhost
        port=1433,  # Default SQL Server port
        database="kaggle_data",  # Default database
        query={
            "driver": "ODBC Driver 17 for SQL Server",  # Ensure ODBC driver matches installed version
            "ConnectionTimeout": "5"  # Set connection timeout to 5 seconds
        }
    )

    try:
        engine = sal.create_engine(connection_url)
        with engine.connect() as connection:
            result = connection.execute(sal.text("SELECT * FROM sys.databases"))
            for row in result:
                print(row)

    except sal.exc.OperationalError as e:
        print(f"Error connecting to the SQL Server: {e}")

    return engine


def append_to_table(engine, conn, df) :
    # create a new table and append data frame values to this table
    try:
        df.to_sql('college_admissions_kaggle', con=engine, if_exists='replace', index=False, chunksize=1000)
    except error as e:
        print(f"Error writing the table: {e}")

    conn.close()


path = '/Users/joannegodwin/Downloads/college_admissions.csv'
data = pd.read_csv(path)

# remove dups
data = data.drop_duplicates()

engine = connect_and_fetch_info()
append_to_table(engine, engine.connect(), data)





