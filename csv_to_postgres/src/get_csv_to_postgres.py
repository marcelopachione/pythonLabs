import os
import logging
import pandas as pd
import psycopg2
import traceback
import urllib.request
from dotenv import load_dotenv


def conn_postgres():
    try:
        # Load python-dotenv
        load_dotenv()

        # Postgres Envs
        postgres_host = os.environ.get("postgres_host")
        postgres_database = os.environ.get("postgres_database")
        postgres_user = os.environ.get("postgres_user")
        postgres_password = os.environ.get("postgres_password")
        postgres_port = os.environ.get("postgres_port")

        conn = psycopg2.connect(
            host=postgres_host,
            database=postgres_database,
            user=postgres_user,
            password=postgres_password,
            port=postgres_port,
        )

        # logging.info('Postgres server connection is successful')

        return conn

    except Exception as e:
        traceback.print_exc()
        logging.error("Couldn't create the Postgres connection")
        return None


def download_file_from_url(url: str, dest_folder: str):
    """
    Download a file from a specific URL and download to the local direcory
    """
    if not os.path.exists(str(dest_folder)):
        os.makedirs(str(dest_folder))  # create folder if it does not exist

    try:
        urllib.request.urlretrieve(url, destination_path)
        logging.info(
            f" CSV file downloaded successfully to the working directory to {destination_path}"
        )
    except Exception as e:
        logging.error(f"Error while downloading the csv file due to: {e}")
        traceback.print_exc()


def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        connection = conn_postgres()

        if connection:
            cur = connection.cursor()

            cur.execute(
                """CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
            Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
            Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)"""
            )

            connection.commit()

            logging.info(
                " New table churn_modelling created successfully on the Postgres server"
            )

    except Exception as e:
        logging.warning(
            " Check if the table churn_modelling exists. Error: {}".format(e)
        )

    finally:
        if connection:
            connection.close()


def write_to_postgres():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    try:
        df = pd.read_csv(f"{dest_folder}/churn_modelling.csv")
        inserted_row_count = 0
        logging.info(f" Datafrane was loaded...")
    except Exception as e:
        logging.error(" Dataframe wasnt load. Error: {}".format(e))
        traceback.print_exc()
        exit()

    try:
        connection = conn_postgres()

        if connection:
            cur = connection.cursor()

        for _, row in df.iterrows():
            count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
            cur.execute(count_query)
            result = cur.fetchone()

            if result[0] == 0:
                inserted_row_count += 1
                cur.execute(
                    """INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
                Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""",
                    (
                        int(row.iloc[0]),
                        int(row.iloc[1]),
                        str(row.iloc[2]),
                        int(row.iloc[3]),
                        str(row.iloc[4]),
                        str(row.iloc[5]),
                        int(row.iloc[6]),
                        int(row.iloc[7]),
                        float(row.iloc[8]),
                        int(row.iloc[9]),
                        int(row.iloc[10]),
                        int(row.iloc[11]),
                        float(row.iloc[12]),
                        int(row.iloc[13]),
                    ),
                )

            connection.commit()

        logging.info(
            f" {inserted_row_count} rows from csv file inserted into churn_modelling table successfully"
        )
    except Exception as e:
        logging.warning(
            " Check if the table churn_modelling exists. Error: {}".format(e)
        )

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    # Load python-dotenv
    load_dotenv()

    url = "https://raw.githubusercontent.com/marcelopachione/pythonLabs/main/datasets/churn_modelling.csv"
    dest_folder = os.environ.get("dest_folder")
    destination_path = f"{dest_folder}/churn_modelling.csv"

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
    )

    conn_postgres()
    download_file_from_url(url, dest_folder)
    create_postgres_table()
    write_to_postgres()
