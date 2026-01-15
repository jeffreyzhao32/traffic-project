import logging

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

GEOJSON_URL = (
    "https://services8.arcgis.com/lYI034SQcOoxRCR7/arcgis/rest/services/"
    "Road_Safety/FeatureServer/0/query"
)
TABLE_NAME = "road_safety_events"
SUPABASE_CONN_ID = "supabase_db"
FILTER_START_DATE = pendulum.now(tz="UTC").subtract(days=5).to_date_string()

def _parse_datetime(value):
    if not value:
        return None
    try:
        return pendulum.parse(value)
    except (TypeError, ValueError):
        return None


def _extract_xy(properties, geometry):
    if properties.get("x") is not None and properties.get("y") is not None:
        return properties.get("x"), properties.get("y")
    if geometry and geometry.get("type") == "Point":
        coords = geometry.get("coordinates") or []
        if len(coords) >= 2:
            return coords[0], coords[1]
    return None, None


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["traffic", "ingestion"],
)
def traffic_ingestion_dag():
    logger = logging.getLogger(__name__)

    @task
    def fetch_geojson():
        params = {
            "outFields": "*",
            "where": (f"\"occ_date\" >= DATE '{FILTER_START_DATE}'"),
            "f": "geojson",
        }
        response = requests.get(GEOJSON_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        features_count = len(payload.get("features", []))
        logger.info("Fetched %d features with params %s", features_count, params)
        return payload

    @task
    def append_new_features(geojson):
        features = geojson.get("features", [])
        if not features:
            return 0

        rows = []
        for feature in features:
            properties = feature.get("properties") or {}
            geometry = feature.get("geometry") or {}
            x_value, y_value = _extract_xy(properties, geometry)
            unique_identifier = properties.get("UniqueIdentifier")
            if not unique_identifier:
                continue
            rows.append(
                (
                    unique_identifier,
                    _parse_datetime(properties.get("occ_date")),
                    properties.get("CollisionDetail"),
                    properties.get("LocationCode"),
                    properties.get("municipality"),
                    properties.get("drug_alcohol"),
                    properties.get("case_type"),
                    x_value,
                    y_value,
                    properties.get("time_est")
                )
            )

        hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                unique_identifier TEXT PRIMARY KEY,
                occurrence_date TIMESTAMPTZ,
                occurrence_detail TEXT,
                location_code TEXT,
                municipality TEXT,
                involve_drug_or_alcohol TEXT,
                road_safety_occurrence_type TEXT,
                x DOUBLE PRECISION,
                y DOUBLE PRECISION,
                time_est TIME
            )
            """
        )

        insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                unique_identifier,
                occurrence_date,
                occurrence_detail,
                location_code,
                municipality,
                involve_drug_or_alcohol,
                road_safety_occurrence_type,
                x,
                y,
                time_est
            ) VALUES %s
            ON CONFLICT (unique_identifier) 
            DO UPDATE SET
                occurrence_date = EXCLUDED.occurrence_date,
                occurrence_detail = EXCLUDED.occurrence_detail,
                location_code = EXCLUDED.location_code,
                municipality = EXCLUDED.municipality,
                involve_drug_or_alcohol = EXCLUDED.involve_drug_or_alcohol,
                road_safety_occurrence_type = EXCLUDED.road_safety_occurrence_type,
                x = EXCLUDED.x,
                y = EXCLUDED.y,
                time_est = EXCLUDED.time_est
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                logger.info("Executing insert into %s with %d rows", TABLE_NAME, len(rows))
                execute_values(cursor, insert_sql, rows)
            conn.commit()

        return len(rows)

    append_new_features(fetch_geojson())


traffic_ingestion_dag()
