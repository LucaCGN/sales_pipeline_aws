# scripts/luigi_pipeline.py

import luigi
import sqlite3
import pandas as pd
import os
import shutil
import numpy as np
import sys
from datetime import datetime

# Append the parent directory to the system path to allow absolute imports
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

class DatabaseTarget(luigi.Target):
    """
    Custom Luigi Target that uses a SQLite database to track task completion.
    
    Parameters:
    - task_name: A unique identifier for the task.
    """

    def __init__(self, task_name):
        self.task_name = task_name
        self.database_path = RDS_PATH  # Using the same RDS_PATH for simplicity
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """
        Ensures that the luigi_task_status table exists in the database.
        """
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS luigi_task_status (
                task_name TEXT PRIMARY KEY,
                completed_at TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def exists(self):
        """
        Checks if the task has been completed by querying the database.
        """
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM luigi_task_status WHERE task_name = ?', (self.task_name,))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def touch(self):
        """
        Marks the task as completed by inserting a record into the database.
        """
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO luigi_task_status (task_name, completed_at)
            VALUES (?, ?)
        ''', (self.task_name, datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

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
        - DatabaseTarget indicating the ExtractData task for this filename is complete.
        """
        return DatabaseTarget(task_name=f'ExtractData_{self.filename}')

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
        extracted_file_path = os.path.join(TEMP_PATH, f'extracted_{self.filename}')
        df.to_csv(extracted_file_path, index=False)
        config.pipeline_logger.info(f"Saved extracted data for {self.filename} to {extracted_file_path}.")

        # Mark the task as complete in the database
        self.output().touch()
        config.pipeline_logger.info(f"Extraction marked complete for {self.filename}.")

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
        - DatabaseTarget indicating the TransformData task for this filename is complete.
        """
        return DatabaseTarget(task_name=f'TransformData_{self.filename}')

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
        extracted_file_path = os.path.join(TEMP_PATH, f'extracted_{self.filename}')
        df = pd.read_csv(extracted_file_path, dtype=str)

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
        transformed_file_path = os.path.join(TEMP_PATH, f'transformed_{self.filename}')
        df.to_csv(transformed_file_path, index=False)
        config.pipeline_logger.info(f"Saved transformed data for {self.filename} to {transformed_file_path}.")

        # Mark the task as complete in the database
        self.output().touch()
        config.pipeline_logger.info(f"Transformation marked complete for {self.filename}.")

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
        Defines the output target for the LoadData task.

        Returns:
        - DatabaseTarget indicating the LoadData task for this filename is complete.
        """
        return DatabaseTarget(task_name=f'LoadData_{self.filename}')

    def run(self):
        """
        Loads the transformed data into the SQLite database.

        Steps:
        - Connects to the database.
        - Iterates over the transformed data and performs upsert operations.
        - Logs insertions and updates.
        - Marks the task as complete in the database upon successful completion.
        """
        config.pipeline_logger.info(f"Starting data load for {self.filename}")
        transformed_file_path = os.path.join(TEMP_PATH, f'transformed_{self.filename}')
        df = pd.read_csv(transformed_file_path)

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

        # Mark the task as complete in the database
        self.output().touch()
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
        Defines the output target for the CleanupTempData task.

        Returns:
        - DatabaseTarget indicating the CleanupTempData task is complete.
        """
        return DatabaseTarget(task_name='CleanupTempData')

    def run(self):
        """
        Removes the 'temp_data' directory and its contents.

        Steps:
        - Deletes the 'temp_data' directory and all its files.
        - Logs the cleanup process.
        - Marks the task as complete in the database upon successful cleanup.
        """
        config.pipeline_logger.info("Starting cleanup of temporary data.")
        try:
            shutil.rmtree(TEMP_PATH)
            config.pipeline_logger.info("Cleaned up temp directory.")
        except Exception as e:
            config.pipeline_logger.error(f"Failed to clean up temp directory: {e}")
            raise e

        # Mark the task as complete in the database
        self.output().touch()
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
        Defines the output target for the RunPipeline task.

        Returns:
        - DatabaseTarget indicating the RunPipeline task is complete.
        """
        return DatabaseTarget(task_name='RunPipeline')

    def run(self):
        """
        Finalizes the pipeline execution.

        Steps:
        - Logs the completion of the pipeline.
        - Marks the task as complete in the database.
        """
        config.pipeline_logger.info("Pipeline execution complete.")

        # Mark the task as complete in the database
        self.output().touch()
        config.pipeline_logger.info("Pipeline run marked as complete.")

class FinalCleanup(luigi.Task):
    """
    Task to perform final cleanup by removing all task status records after the pipeline completes.

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
        Defines the output target for the FinalCleanup task.

        Returns:
        - DatabaseTarget indicating the FinalCleanup task is complete.
        """
        return DatabaseTarget(task_name='FinalCleanup')

    def run(self):
        """
        Removes task status records from the database to clean up.

        Steps:
        - Deletes specific task records from the luigi_task_status table.
        - Logs the final cleanup process.
        - Marks the task as complete in the database upon successful cleanup.
        """
        config.pipeline_logger.info("Starting final cleanup of task status records.")
        try:
            conn = sqlite3.connect(RDS_PATH)
            cursor = conn.cursor()
            # Define the tasks to remove from the status table
            tasks_to_remove = [
                'RunPipeline',
                'CleanupTempData',
                'FinalCleanup',
                'CompletePipeline'
                # Add other task names if necessary
            ]
            for task_name in tasks_to_remove:
                cursor.execute('DELETE FROM luigi_task_status WHERE task_name = ?', (task_name,))
                config.pipeline_logger.info(f"Removed task status for: {task_name}")
            conn.commit()
        except Exception as e:
            config.pipeline_logger.error(f"Failed during final cleanup: {e}")
            raise e
        finally:
            conn.close()

        # Mark the task as complete in the database
        self.output().touch()
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
        Defines the output target for the CompletePipeline task.

        Returns:
        - DatabaseTarget indicating the CompletePipeline task is complete.
        """
        return DatabaseTarget(task_name='CompletePipeline')

    def run(self):
        """
        Marks the completion of the entire pipeline and final cleanup.

        Steps:
        - Logs the completion.
        - Marks the task as complete in the database.
        """
        config.pipeline_logger.info("Entire pipeline and final cleanup complete.")

        # Mark the task as complete in the database
        self.output().touch()
        config.pipeline_logger.info("Complete pipeline run marked as complete.")

if __name__ == '__main__':
    # Run the pipeline using Luigi's build function
    luigi.build([CompletePipeline()], local_scheduler=True)
