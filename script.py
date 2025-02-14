
from dagster import repository
from dagster_demo_2 import process_data_job
from dagster_demo import my_first_job

@repository
def my_repo():
    return [process_data_job, my_first_job]


'''if __name__ == "__main__":

    result1 = execute_job(process_data)
    if result1.success:
        print("Job succeeded!")
    else:
        print("Job failed!")

    result2 = execute_job(my_first_job)
    if result2.success:
        print("Job succeeded!")
    else:
        print("Job failed!")'''
