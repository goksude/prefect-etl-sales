import os
import kaggle
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from prefect import task, flow, get_run_logger
import zipfile

load_dotenv()

os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

connection_string = os.getenv("ETL_CONNECTION_STRING")
engine = create_engine(connection_string)



@task
def download_data_task(dataset_slug: str, download_path: str = "."):
    # Downloads a dataset from Kaggle and returns the path to the zip file.
    logger = get_run_logger()

    logger.info("Authenticating with Kaggle API...")
    kaggle.api.authenticate()

    logger.info(f"Downloading dataset '{dataset_slug}'...")
    kaggle.api.dataset_download_files(dataset_slug, path=download_path, unzip=False, quiet=False)

    zip_file_name = f"{dataset_slug.split('/')[1]}.zip"
    zip_file_path = os.path.join(download_path, zip_file_name)
    logger.info(f"Dataset successfully downloaded to '{zip_file_path}'.")
    return zip_file_path


@task
def extract_and_load_raw_task(zip_file_path: str):
    # Extracts a CSV from a zip file and loads to raw_sales table
    logger = get_run_logger()

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            csv_file_name = zf.namelist()[0]
            zf.extract(csv_file_name)
            logger.info(f"Extracted '{csv_file_name}' from zip file.")
    except (zipfile.BadZipFile, IndexError) as e:
        logger.error(f"Failed to extract from zip file: {e}")
        raise

    raw_table_name = 'raw_sales'

    logger.info(f"Reading data from '{csv_file_name}'...")
    df = pd.read_csv(csv_file_name)
    logger.info(f"Found {len(df)} rows in the CSV file.")

    logger.info(f"Loading data into raw table '{raw_table_name}'...")
    df.to_sql(raw_table_name, engine, if_exists='replace', index=False)
    logger.info("Raw data successfully loaded into the database.")

    # Clean up the extracted CSV file
    os.remove(csv_file_name)
    logger.info(f"Cleaned up extracted file: '{csv_file_name}'.")


@task
def transform_data_task():
        # Calls stored procedure in PostgreSQL to transform data
    logger = get_run_logger()

    logger.info("Executing stored procedure 'transform_and_load_sales'...")
    try:
        with engine.connect() as connection:
            connection.execute(text('CALL transform_and_load_sales();'))
            connection.commit()
        logger.info("Data transformation completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}")
        raise



@flow
def sales_etl_flow():

    logger = get_run_logger()
    logger.info("--- Starting Sales ETL Flow ---")

    dataset_slug = "rohitsahoo/sales-forecasting"

    zip_path = download_data_task(dataset_slug=dataset_slug)
    extract_and_load_raw_task(zip_file_path=zip_path)
    transform_data_task()

    logger.info("--- Sales ETL Flow finished successfully. ---")

if __name__ == "__main__":
    sales_etl_flow()