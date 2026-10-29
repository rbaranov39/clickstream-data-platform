import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="git_sync_probe",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=False,
)
def git_sync_probe():
    @task
    def show_version():
        print("Version 1")

    show_version()


git_sync_probe()