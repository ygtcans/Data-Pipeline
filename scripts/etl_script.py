import logging
from src.data_io_manager import PostgresDataHandler, LocalDataHandler
from src.data_cleaner import DataCleaner
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

class ETLProcess:
    """
    Class to handle the ETL (Extract, Transform, Load) process.
    """
    def __init__(self, input_path: str, table_name: str):
        """
        Initializes the ETL process with the input path, output path, and PostgreSQL table name.

        Args:
            input_path (str): Path to the input file.
            table_name (str): Name of the PostgreSQL table for loading data.
        """
        self.input_path = input_path
        self.table_name = table_name
        self.local_handler = LocalDataHandler()
        self.postgres_handler = PostgresDataHandler()

    def extract(self):
        """
        Extracts data from a local file.
        
        Returns:
            pd.DataFrame: Extracted DataFrame.
        """
        logging.info("Starting extraction process.")
        try:
            df = self.local_handler.read(self.input_path)
            logging.info(f"Data extracted successfully from {self.input_path}.")
            return df
        except Exception as e:
            logging.error(f"Failed to extract data: {e}")
            raise

    def transform(self, df):
        """
        Transforms the extracted data.
        
        Args:
            df (pd.DataFrame): DataFrame to be transformed.

        Returns:
            pd.DataFrame: Transformed DataFrame.
        """
        logging.info("Starting transformation process.")
        try:
            cleaner = DataCleaner(df)
            cleaned_df = cleaner.clean_data(strategy='mode')  # or 'mode', based on your needs
            logging.info("Data transformation completed successfully.")
            return cleaned_df
        except Exception as e:
            logging.error(f"Failed to transform data: {e}")
            raise

    def load(self, df):
        """
        Loads the transformed data into the PostgreSQL database.
        
        Args:
            df (pd.DataFrame): DataFrame to be loaded.
        """
        logging.info("Starting loading process.")
        try:
            self.postgres_handler.write(df, self.table_name)
            logging.info(f"Data loaded successfully into table '{self.table_name}'.")
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            raise

    def run(self):
        """
        Runs the complete ETL process (extract, transform, and load).
        """
        try:
            df = self.extract()
            transformed_df = self.transform(df)
            self.load(transformed_df)
            logging.info("ETL process completed successfully.")
        except Exception as e:
            logging.error(f"ETL process failed: {e}")

# Run the ETL process if this script is executed directly
if __name__ == "__main__":
    input_path = os.getenv("INPUT_FILE_PATH")  
    table_name = os.getenv("POSTGRES_TABLE_NAME")

    etl = ETLProcess(input_path, table_name)
    etl.run()
