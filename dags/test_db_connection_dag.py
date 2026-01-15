import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

SUPABASE_CONN_ID = "supabase_db"
TABLE_NAME = "test_airflow_table"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test", "connection"],
)
def test_db_connection_dag():
    @task
    def test_connection():
        hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                unique_identifier TEXT PRIMARY KEY
            )
            """
        )
        return hook.get_first("SELECT 1;")

    test_connection()


test_db_connection_dag()
