from dagster import execute_job
from dagster_demo_2 import data_pipeline
from dagster_demo import my_first_job

if __name__ == "__main__":
    result1 = execute_job(data_pipeline)
    result2 = execute_job(my_first_job)

    print(f"Pipeline 1 success: {result1.success}")
    print(f"Pipeline 2 success: {result2.success}")
