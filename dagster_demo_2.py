import os
import pandas as pd
from dagster import job, op, Out, In, execute_job, Field

@op(
    config_schema={"input_file_path": Field(str, default_value="data/co2_emissions.csv")},
    out=Out(pd.DataFrame)
)
def read_csv(context):
    input_file_path = context.op_config["input_file_path"]
    try:
        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"{input_file_path} does not exist.")
        df = pd.read_csv(input_file_path)
        context.log.info(f"Read {len(df)} rows from {input_file_path}")
        return df
    except Exception as e:
        context.log.error(f"Failed to read {input_file_path}: {e}")
        raise

@op(ins={"df": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def process_data(context, df: pd.DataFrame):
    try:
        df['new_column'] = df['CO2_Emissions'] * 2  # Example transformation
        context.log.info(f"Processed data: Added new_column")
        return df
    except Exception as e:
        context.log.error(f"Data processing failed: {e}")
        raise

@job
def process_data_job():
    df = read_csv()
    process_data(df)

def run_job():
    result = execute_job(
        process_data_job,
        run_config={
            "ops": {
                "read_csv": {
                    "config": {
                        "input_file_path": "data/co2_emissions.csv"
                    }
                }
            }
        }
    )
    if result.success:
        print("Job completed successfully!")
    else:
        print("Job failed. Check the logs for details.")

if __name__ == "__main__":
    run_job()
