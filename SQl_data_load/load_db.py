from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pandas as pd
import joblib
from s3fs.core import S3FileSystem

def load_data():
    # Initialize S3 connection
    s3 = S3FileSystem(anon=False)
    DIR_wh = 's3://ece5984-s3-prathamj//Bank_ETL/reports'  # S3 bucket directory
    DIR_par = 's3://ece5984-s3-prathamj//Bank_ETL/features'
    try:
        # Load .pkl files using joblib
        with s3.open(f"{DIR_wh}/fraud_report_with_cities.pkl", 'rb') as f:
            fraud_report_with_cities = joblib.load(f)
        print("Loaded fraud_report_with_cities.pkl successfully.")

        with s3.open(f"{DIR_wh}/city_fraud_report.pkl", 'rb') as f:
            city_fraud_report = joblib.load(f)
        print("Loaded city_fraud_report.pkl successfully.")

        # Load .parquet file
        fraud_large_transactions = pd.read_parquet(f"{DIR_par}/fraud_large_transactions.parquet", storage_options={'anon': False})
        print("Loaded fraud_large_transactions.parquet successfully.")

    except Exception as e:
        print(f"Error loading files from S3: {e}")
        return

    # MySQL credentials
    user = "admin"
    pw = "9G>c(JolkKDkpanP:jBfp?P+<{yJ"
    endpnt = "data-eng-db.cluster-cwgvgleixj0c.us-east-1.rds.amazonaws.com"
    db_name = "atharva47"

    try:
        # Connect to MySQL server
        engine = create_engine(f"mysql+pymysql://{user}:{pw}@{endpnt}")


        with engine.connect() as connection:
            db_exists = connection.execute(f"SHOW DATABASES LIKE '{db_name}';").fetchone()
            if not db_exists:
                connection.execute(f"CREATE DATABASE {db_name}")

        # Reconnect to the specific database
        engine = create_engine(f"mysql+pymysql://{user}:{pw}@{endpnt}/{db_name}")

        # Inserting DataFrames into the MySQL database
        print("Inserting data into MySQL...")
        fraud_report_with_cities.to_sql('fraud_report_with_cities', con=engine, if_exists='replace', index=False, chunksize=1000)
        city_fraud_report.to_sql('city_fraud_report', con=engine, if_exists='replace', index=False, chunksize=1000)
        fraud_large_transactions.to_sql('fraud_large_transactions', con=engine, if_exists='replace', index=False, chunksize=1000)
        print("Data successfully loaded into MySQL database.")

    except OperationalError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")

if __name__ == "__main__":
    load_data()
