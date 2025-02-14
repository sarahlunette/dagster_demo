from dagster import job, op

@op
def hello():
    return "Hello, Dagster!"

@job
def my_first_job():
    hello()
