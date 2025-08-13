from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Latitude and longitude constants for the desired location (London in this case)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner':'airflow',
    'start_date':days_ago(1)
}

## DAG
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_inteval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""
        
        # Use HTTP Hook to get the weather data
        
        http_hook=HttpHook(http_conn_id=API_CONN_ID, method='GET')
        
        
        ## Build the API endpoint
        endpoint= f'https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=TRUE'
        
        ## Make the request via the HTTP hook
        response=http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
    