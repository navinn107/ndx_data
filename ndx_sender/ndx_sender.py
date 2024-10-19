import requests
import json
import configparser
import os
from datetime import datetime
from amqp import AMQP
import logging as log
from apscheduler.schedulers.blocking import BlockingScheduler  

# Configuration for RabbitMQ and file paths
exchange_name = "ndx_exchange"
routing_key = "ndx_exchange/.784b5f80-eef8-4a9e-bc35-8ce7f940007d"
ri_uuid = "784b5f80-eef8-4a9e-bc35-8ce7f940007d"

# Load configurations
config = configparser.ConfigParser()
config.read("config.ini")

# Initialize AMQP connection
amqp = AMQP(config)
amqp.connect(exchange_name, routing_key, ri_uuid)

# Logging setup
log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class openDataTelangana:
    
    def __init__(self, api, conditions, headers, index_file):
        self.api = api
        self.conditions = conditions
        self.headers = headers
        self.index_file = index_file

    def get_data(self):
        self.current_value = self.load_current_value()
        url = self.api["base_url"].format(self.current_value)

        payload = json.dumps({
            "conditions": [
                {
                    "resource": self.conditions["resource"],
                    "property": self.conditions["property"],
                    "value": self.conditions["value"],
                    "operator": self.conditions["operator"]
                }
            ]
        })

        headers = {
            'accept': self.headers["accept"],
            'Content-Type': self.headers["Content-Type"]
        }

        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()

        except requests.exceptions.HTTPError as errh:
            log.critical("HTTP Error occurred: %s", errh)

        except requests.exceptions.ConnectionError as errc:
            log.critical("Connection Error occurred: %s", errc)

        except requests.exceptions.Timeout as errt:
            log.critical("Timeout Error occurred: %s", errt)

        except requests.exceptions.RequestException as err:
            log.critical("An unknown Request Error occurred: %s", err)

        except Exception as oe:
            log.critical("Unknown exception occurred: %s", oe)

        else:
            if response.status_code == 200:
                json_array = response.json()["results"]
                self.transform_data(json_array)
                self.increment_value()  # Increment the value after successful transformation

    def transform_data(self, json_array):
        for packet in json_array:
            observationDateTime = lambda date_string: datetime.strptime(date_string, '%d-%b-%y').strftime("%Y-%m-%dT%H:%M:%S+05:30")

            weather_data = {
                "ri_id": ri_uuid,
                "districtName": packet.get("district", None),
                "subdistrictName": packet.get("mandal", None),
                "observationDateTime": observationDateTime(packet["date"]),
                "precipitation": float(packet.get("rain_mm")) if packet.get("rain_mm") else None,
                "airTemperature_maxOverTime": float(packet.get("max_temp_c")) if packet.get("max_temp_c") else None,
                "airTemperature_minOverTime": float(packet.get("min_temp_c")) if packet.get("min_temp_c") else None,
                "relativeHumidity_maxOverTime": float(packet.get("max_humidity")) if packet.get("max_humidity") else None,
                "relativeHumidity_minOverTime": float(packet.get("min_humidity")) if packet.get("min_humidity") else None,
                "windSpeed_maxOverTime": float(packet.get("max_wind_speed_kmph")) if packet.get("max_wind_speed_kmph") else None,
                "windSpeed_minOverTime": float(packet.get("min_wind_speed_kmph")) if packet.get("min_wind_speed_kmph") else None
            }

            amqp.publish_data(weather_data)

        self.increment_value()

    def load_current_value(self):
        if os.path.exists(self.index_file):
            with open(self.index_file, 'r') as file:
                return int(file.read().strip())
        else:
            with open(self.index_file, "w") as f:
                f.write(self.api['initial_value'])
            return int(self.api['initial_value'])

    def save_current_value(self):
        with open(self.index_file, 'w') as file:
            file.write(str(self.current_value))

    def increment_value(self):
        self.current_value += 1
        self.save_current_value()

if __name__ == "__main__":
    api = config["API"]
    conditions = config["Conditions"]
    headers = config["Headers"]
    index_file = config["file_name"]["index_file"]

    open_data_telangana = openDataTelangana(api, conditions, headers, index_file)
    open_data_telangana.get_data()

    scheduler = BlockingScheduler()
    scheduler.add_job(open_data_telangana.get_data, "cron",  month = "*",day = "10", hour="11", minute="30", max_instances=5, coalesce=True, misfire_grace_time=300)
    scheduler.start()
