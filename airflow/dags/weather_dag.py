from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import pandas as pd

def kelvin_to_farenheit(temp_in_kelvin):
    temp_in_farenheit = (temp_in_kelvin - 273.15) * (9/5) * 32
    return temp_in_farenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids = 'extract_weather_data')
    city = data['name']
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_farenheit(data['main']['temp'])
    feels_like_farenheit= kelvin_to_farenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_farenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_farenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                    "Description": weather_description,
                    "Temperature (F)": temp_farenheit,
                    "Feels Like (F)": feels_like_farenheit,
                    "Minimun Temp (F)":min_temp_farenheit,
                    "Maximum Temp (F)": max_temp_farenheit,
                    "Pressure": pressure,
                    "Humidty": humidity,
                    "Wind Speed": wind_speed,
                    "Time of Record": time_of_record,
                    "Sunrise (Local Time)":sunrise_time,
                    "Sunset (Local Time)": sunset_time                        
                    }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    #df_data.to_csv(f'{dt_string}.csv', index=False)
    df_data.to_csv(f's3://weather-api-airflow/{dt_string}.csv', header=True, index=False)


default_args = {
    'owner' : 'ariflow',
    'depends_on_past' : False,
    'start_date' : datetime(2023, 11, 4),
    'email' : ['pooja@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries': 2,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG ('weather_dag',
          default_args = default_args,
          schedule_interval=timedelta(days=1),
          catchup = False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=b8ffdf76a6f40774ae297960829947fc'
    )
    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=b8ffdf76a6f40774ae297960829947fc',
        method='GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable = transform_load_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
