import pandas as pd
from dagster import job, op, Out, Output
from dagster.core.definitions import MetadataValue
from dagster.fs import fs_io_manager

# Op to read a CSV file
@op(out=Out(pd.DataFrame))
def read_csv(context, file_path: str):
    try:
        df = pd.read_csv(file_path)
        context.log.info(f"Read {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        context.log.error(f"Failed to read {file_path}: {e}")
        raise

# Op to process the data (e.g., filter and add a new column)
@op(out=Out(pd.DataFrame))
def process_data(context, df: pd.DataFrame):
    try:
        # Perform some data transformations
        df['new_column'] = df['value'] * 2  # Example transformation
        context.log.info(f"Processed data: Added new_column")
        return df
    except Exception as e:
        context.log.error(f"Data processing failed: {e}")
        raise

# Op to write the processed data to a new CSV file
@op
def write_csv(context, df: pd.DataFrame, output_path: str):
    try:
        df.to_csv(output_path, index=False)
        context.log.info(f"Wrote data to {output_path}")
    except Exception as e:
        context.log.error(f"Failed to write {output_path}: {e}")
        raise

# Defining a job with these operations
@job(resource_defs={"io_manager": fs_io_manager})
def data_pipeline():
    # Define file paths
    input_file_path = "input_data.csv"
    output_file_path = "output_data.csv"
    
    # Define the pipeline
    data = read_csv(input_file_path)
    processed_data = process_data(data)
    write_csv(processed_data, output_file_path)

