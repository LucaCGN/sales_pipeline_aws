# scripts/luigi_pipeline.py

import luigi
import sqlite3
import pandas as pd
import os
import shutil
import numpy as np
import sys
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.config import config  # Correct absolute import

# Initialize the pipeline logger
config.pipeline_logger.info("Starting the Luigi pipeline...")

try:
    # Placeholder for any initialization code
    config.pipeline_logger.info("Luigi pipeline initialized successfully.")
except Exception as e:
    config.pipeline_logger.error(f"Luigi pipeline initialization failed: {e}")

# Corrected Paths representing AWS services using config
S3_BUCKET_PATH = config.BUCKET_PATH          # Emulates AWS S3 Bucket
RDS_PATH = config.DATABASE_PATH              # Emulates AWS RDS
TEMP_PATH = config.TEMP_DATA_PATH            # For intermediate data files

class ExtractData(luigi.Task):
    """
    Task to extract data from a single CSV file in the 'bucket' directory.

    AWS Parallel:
    - Emulates data extraction from an S3 bucket (AWS S3).
    - The 'bucket' directory represents an S3 bucket where raw data files are stored.

    Parameters:
    - filename: Name of the CSV file to process.
    """
    filename = luigi.Parameter()

    def output(self):
        """
        Defines the output target for the extracted data.

        Returns:
        - Luigi LocalTarget pointing to the extracted CSV file in the 'temp_data' directory.
        """
        return luigi.LocalTarget(os.path.join(TEMP_PATH, f'extracted_{self.filename}'))

    def run(self):
        """
        Reads the CSV file from the 'bucket' directory, cleans it, and saves the cleaned data to the 'temp_data' directory.

        Steps:
        - Reads the CSV file, inferring the delimiter and handling bad lines.
        - Standardizes headers by stripping whitespace and converting to lowercase.
        - Ensures all required columns are present, adding missing ones as empty strings.
        - Saves the cleaned data to the 'temp_data' directory.
        """
        # Ensure the temp directory exists
        os.makedirs(TEMP_PATH, exist_ok=True)
        file_path = os.path.join(S3_BUCKET_PATH, self.filename)
        config.pipeline_logger.info(f"Starting extraction for {self.filename}")
        try:
            # Read the file with pandas, letting pandas infer the delimiter and handle bad lines
            df = pd.read_csv(
                file_path,
                sep=None,
                engine='python',
                skip_blank_lines=True,
                on_bad_lines='skip',  # Skip bad lines
                dtype=str  # Read all columns as strings initially
            )

            # Standardize headers by stripping whitespace and converting to lowercase
            df.columns = [col.strip().lower() for col in df.columns]

            # Define the required columns
            required_columns = ['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'order_date']

            # Check if all required columns are present
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                config.pipeline_logger.warning(f"Columns {missing_columns} are missing in {self.filename}. Filling with empty strings.")
                for col in missing_columns:
                    df[col] = ''

            # Keep only required columns
            df = df[required_columns]

            config.pipeline_logger.info(f"Extracted and cleaned file {self.filename} successfully.")
        except Exception as e:
            config.pipeline_logger.error(f"Failed to extract {self.filename}: {e}")
            raise e

        # Save the extracted data
        df.to_csv(self.output().path, index=False)
        config.pipeline_logger.info(f"Saved extracted data for {self.filename} to {self.output().path}.")


class TransformData(luigi.Task):
    """
    Task to transform the extracted data from a single CSV file.

    AWS Parallel:
    - Represents data transformation steps that might occur in AWS Glue or AWS Data Pipeline.
    - Normalizes and cleans data to prepare it for loading into the database.

    Parameters:
    - filename: Name of the CSV file to process.
    """

    filename = luigi.Parameter()

    def requires(self):
        """
        Specifies that this task depends on the successful completion of the ExtractData task for the same file.
        """
        return ExtractData(filename=self.filename)

    def output(self):
        """
        Defines the output target for the transformed data.

        Returns:
        - Luigi LocalTarget pointing to the transformed CSV file in the 'temp_data' directory.
        """
        return luigi.LocalTarget(os.path.join(TEMP_PATH, f'transformed_{self.filename}'))

    def run(self):
        """
        Transforms the extracted data to ensure consistency and correctness.

        Steps:
        - Normalizes decimal separators in 'price' and 'quantity' columns.
        - Removes trailing spaces and unwanted characters in string columns.
        - Converts data types with error handling.
        - Handles missing or malformed dates.
        - Removes rows with missing critical values.
        - Saves the transformed data to the 'temp_data' directory.
        """
        config.pipeline_logger.info(f"Starting transformation for {self.filename}")
        df = pd.read_csv(self.input().path, dtype=str)

        # Normalize decimal separators in 'price' and 'quantity' columns
        for col in ['price', 'quantity']:
            if col in df.columns:
                df[col] = df[col].str.replace(',', '.').str.strip()

        # Remove trailing spaces and unwanted characters from string columns
        for col in ['order_id', 'customer_id', 'product_id']:
            if col in df.columns:
                df[col] = df[col].str.strip().str.replace(r'\s+', '', regex=True)

        # Convert data types with error handling
        dtype_corrections = {
            'order_id': str,
            'customer_id': str,
            'product_id': str,
            'quantity': float,
            'price': float,
            'order_date': str
        }
        for column, dtype in dtype_corrections.items():
            if column in df.columns:
                try:
                    if dtype == float:
                        df[column] = pd.to_numeric(df[column].str.replace(' ', ''), errors='coerce').fillna(0.0)
                    else:
                        df[column] = df[column].astype(dtype)
                    config.pipeline_logger.info(f"Converted column {column} to {dtype.__name__}.")
                except ValueError as e:
                    config.pipeline_logger.warning(f"Data type conversion failed for column {column}: {e}")
                    if dtype == float:
                        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)
                    else:
                        df[column] = df[column].fillna('')
            else:
                config.pipeline_logger.warning(f"Column '{column}' is missing from the data. Filling with default values.")
                if dtype == float:
                    df[column] = 0.0
                else:
                    df[column] = ''

        # Replace empty strings and 'nan' strings with NaN in critical columns
        critical_columns = ['order_id', 'customer_id', 'product_id']
        df[critical_columns] = df[critical_columns].replace({'': np.nan, 'nan': np.nan})

        # Remove rows with missing critical values
        initial_row_count = len(df)
        df.dropna(subset=critical_columns, inplace=True)
        final_row_count = len(df)
        config.pipeline_logger.info(f"Dropped {initial_row_count - final_row_count} rows with missing critical values in {self.filename}.")

        # Handle missing or malformed dates
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        if df['order_date'].isnull().any():
            config.pipeline_logger.warning(f"Some 'order_date' values in {self.filename} could not be parsed. Filling with today's date.")
            df['order_date'] = df['order_date'].fillna(pd.Timestamp.today())

        # Save transformed data
        df.to_csv(self.output().path, index=False)
        config.pipeline_logger.info(f"Saved transformed data for {self.filename} to {self.output().path}.")


class LoadData(luigi.Task):
    """
    Task to load transformed data from a single CSV file into the database.

    AWS Parallel:
    - Emulates loading data into AWS RDS or any relational database.
    - Implements upsert logic to insert new records or update existing ones.

    Parameters:
    - filename: Name of the CSV file to process.
    """

    filename = luigi.Parameter()

    def requires(self):
        """
        Specifies that this task depends on the successful completion of the TransformData task for the same file.
        """
        return TransformData(filename=self.filename)

    def output(self):
        """
        Dummy output to satisfy Luigi's requirement.

        Returns:
        - Luigi LocalTarget pointing to a marker file indicating the data load is complete.
        """
        return luigi.LocalTarget(os.path.join(TEMP_PATH, f'load_complete_{self.filename}'))

    def run(self):
        """
        Loads the transformed data into the SQLite database.

        Steps:
        - Connects to the database.
        - Iterates over the transformed data and performs upsert operations.
        - Logs insertions and updates.
        - Creates an output marker file upon successful completion.
        """
        config.pipeline_logger.info(f"Starting data load for {self.filename}")
        df = pd.read_csv(self.input().path)

        # Connect to the database
        conn = sqlite3.connect(RDS_PATH)
        cursor = conn.cursor()

        try:
            for _, row in df.iterrows():
                try:
                    # Perform upsert operation
                    cursor.execute('''
                    INSERT INTO sales (order_id, customer_id, product_id, quantity, price, order_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        customer_id=excluded.customer_id,
                        product_id=excluded.product_id,
                        quantity=excluded.quantity,
                        price=excluded.price,
                        order_date=excluded.order_date
                    ''', (
                        row['order_id'],
                        row['customer_id'],
                        row['product_id'],
                        row['quantity'],
                        row['price'],
                        row['order_date']
                    ))

                    # Log the operation
                    config.database_logger.info(f"Upserted order_id {row['order_id']} with values: {row.to_dict()}")
                except Exception as e:
                    config.database_logger.error(f"Error inserting/updating order_id {row['order_id']}: {e}")

            conn.commit()
            config.pipeline_logger.info(f"Successfully loaded data from {self.filename}.")
        except Exception as e:
            config.pipeline_logger.error(f"Failed to load data from {self.filename}: {e}")
            conn.rollback()
            raise e
        finally:
            conn.close()

        # Create the output marker file
        with self.output().open('w') as f:
            f.write('Data load complete.')
        config.pipeline_logger.info(f"Data load marked complete for {self.filename}.")


class CleanupTempData(luigi.Task):
    """
    Task to clean up the 'temp_data' directory after all processing is done.

    AWS Parallel:
    - Represents cleanup operations that might occur after data processing in AWS Glue or AWS Batch.
    - Ensures that temporary data does not consume unnecessary storage.

    Dependencies:
    - This task depends on the completion of all LoadData tasks.
    """

    def requires(self):
        """
        Collects all LoadData tasks for each CSV file in the 'bucket' directory.
        """
        csv_files = [f for f in os.listdir(S3_BUCKET_PATH) if f.endswith('.csv')]
        return [LoadData(filename=csv_file) for csv_file in csv_files]

    def output(self):
        """
        Marker file indicating cleanup is done.

        Returns:
        - Luigi LocalTarget pointing to a marker file indicating the cleanup is complete.
        """
        # Place the marker file in the logs directory to avoid being deleted
        return luigi.LocalTarget(os.path.join(config.LOGS_PATH, 'cleanup_complete.txt'))

    def run(self):
        """
        Removes the 'temp_data' directory and its contents.

        Steps:
        - Deletes the 'temp_data' directory and all its files.
        - Logs the cleanup process.
        - Creates a marker file upon successful cleanup.
        """
        config.pipeline_logger.info("Starting cleanup of temporary data.")
        try:
            shutil.rmtree(TEMP_PATH)
            config.pipeline_logger.info("Cleaned up temp directory.")
        except Exception as e:
            config.pipeline_logger.error(f"Failed to clean up temp directory: {e}")
            raise e

        # Create the output marker file in the logs directory
        with self.output().open('w') as f:
            f.write('Temp data cleanup complete.')
        config.pipeline_logger.info("Temporary data cleanup marked as complete.")


class RunPipeline(luigi.Task):
    """
    Final task to run the entire pipeline and perform cleanup.

    AWS Parallel:
    - Represents the orchestration of the entire data pipeline, similar to AWS Step Functions orchestrating multiple AWS services.
    """

    def requires(self):
        """
        Specifies that this task depends on the completion of the CleanupTempData task.
        """
        return CleanupTempData()

    def output(self):
        """
        Marker file indicating the entire pipeline has run.

        Returns:
        - Luigi LocalTarget pointing to a marker file indicating the pipeline run is complete.
        """
        # Since TEMP_PATH has been removed by CleanupTempData, place the marker in the logs directory
        return luigi.LocalTarget(os.path.join(config.LOGS_PATH, 'pipeline_run_complete.txt'))

    def run(self):
        """
        Finalizes the pipeline execution.

        Steps:
        - Logs the completion of the pipeline.
        - Creates a marker file indicating the pipeline run is complete.
        """
        config.pipeline_logger.info("Pipeline execution complete.")
        with self.output().open('w') as f:
            f.write('Pipeline run complete.')
        config.pipeline_logger.info("Pipeline run marked as complete.")


class FinalCleanup(luigi.Task):
    """
    Task to perform final cleanup by removing all marker files after the pipeline completes.

    Dependencies:
    - This task depends on the completion of the RunPipeline task.
    """

    def requires(self):
        """
        Specifies that this task depends on the completion of the RunPipeline task.
        """
        return RunPipeline()

    def output(self):
        """
        Marker file indicating final cleanup is done.

        Returns:
        - Luigi LocalTarget pointing to a marker file indicating the final cleanup is complete.
        """
        # return luigi.LocalTarget(os.path.join(config.LOGS_PATH, 'final_cleanup_complete.txt'))

    def run(self):
        """
        Removes the specified marker files.

        Steps:
        - Deletes 'pipeline_run_complete.txt' from the logs directory.
        - Deletes 'cleanup_complete.txt' from the logs directory.
        - Logs the final cleanup process.
        - Creates a final cleanup marker file.
        """
        config.pipeline_logger.info("Starting final cleanup of marker files.")
        try:
            # Remove the pipeline run complete marker file
            pipeline_marker = os.path.join(config.LOGS_PATH, 'pipeline_run_complete.txt')
            if os.path.exists(pipeline_marker):
                os.remove(pipeline_marker)
                config.pipeline_logger.info(f"Removed marker file: {pipeline_marker}")

            # Remove the cleanup complete marker file
            cleanup_marker = os.path.join(config.LOGS_PATH, 'cleanup_complete.txt')
            if os.path.exists(cleanup_marker):
                os.remove(cleanup_marker)
                config.pipeline_logger.info(f"Removed marker file: {cleanup_marker}")
        except Exception as e:
            config.pipeline_logger.error(f"Failed during final cleanup: {e}")
            raise e

        # Create the output marker file indicating final cleanup is complete
        with self.output().open('w') as f:
            f.write('Final cleanup complete.')
        config.pipeline_logger.info("Final cleanup marked as complete.")


class CompletePipeline(luigi.Task):
    """
    Task to ensure the entire pipeline runs and final cleanup is performed.

    AWS Parallel:
    - Represents the orchestration of running the pipeline and performing all cleanup tasks.
    """

    def requires(self):
        """
        Specifies that this task depends on the completion of the FinalCleanup task.
        """
        return FinalCleanup()

    def output(self):
        """
        Marker file indicating the entire pipeline and final cleanup is complete.

        Returns:
        - Luigi LocalTarget pointing to a marker file indicating the complete pipeline run is done.
        """
        # return luigi.LocalTarget(os.path.join(config.LOGS_PATH, 'complete_pipeline_run.txt'))

    def run(self):
        """
        Marks the completion of the entire pipeline and final cleanup.

        Steps:
        - Logs the completion.
        - Creates a final marker file indicating everything is complete.
        """
        config.pipeline_logger.info("Entire pipeline and final cleanup complete.")
        with self.output().open('w') as f:
            f.write('Complete pipeline run and cleanup.')
        config.pipeline_logger.info("Complete pipeline run marked as complete.")


if __name__ == '__main__':
    # Run the pipeline using Luigi's build function
    luigi.build([CompletePipeline()], local_scheduler=True)
