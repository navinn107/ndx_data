

import pika
import psycopg2
import json
import time
import configparser
import logging as log

# Configuration for RabbitMQ and file paths
exchange_name = "ndx_exchange"
routing_key = "ndx_exchange/.784b5f80-eef8-4a9e-bc35-8ce7f940007d"
ri_uuid = "784b5f80-eef8-4a9e-bc35-8ce7f940007d"

# Load configurations
config = configparser.ConfigParser()
config.read("config.ini")

# Logging setup
log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
class ndxReceiver:

    def __init__(self, config):
        
        """Initialize the AMQP client and load general configurations from the config file."""

        # Load RabbitMQ connection configurations from the config file
        self.rabbitmq_user = config['RabbitMQ']['rabbitmq_user']
        self.rabbitmq_password = config['RabbitMQ']['rabbitmq_password']
        self.rabbitmq_port = int(config['RabbitMQ']['rabbitmq_port'])
        self.rabbitmq_host = config['RabbitMQ']['rabbitmq_host']

        self.psql_dbname = config["postgresql"]["dbname"]
        self.psql_user = config["postgresql"]["user"]
        self.psql_user = config["postgresql"]["password"]
        self.psql_password = config["postgresql"]["host"]
        self.psql_host = config["postgresql"]["port"]

        self.connection = None
        self.channel = None


        
# [postgresql]
# dbname = ndx_db
# user = postgres
# password = admin
# host = 3.109.212.19
# port = 5432



# # RabbitMQ connection details
# rabbitmq_user = "navin"
# rabbitmq_password = "M@thematics107"
# rabbitmq_host = "3.109.212.19"
# rabbitmq_port = 5672
# queue_name = "784b5f80-eef8-4a9e-bc35-8ce7f940007d"


psql_connection = psycopg2.connect(
    dbname="ndx_db",
    user="postgres",
    password="admin",
    host="3.109.212.19",
    port="5432"
)

cursor = psql_connection.cursor()

print("connection established")

# SQL query to insert data
insert_query = """
    INSERT INTO ndx_data(
        ri_id,
        district_name,
        subdistrict_name,
        observation_datetime,
        windspeed_min_over_time,
        windspeed_max_over_time,
        relative_humidity_min_over_time,
        relative_humidity_max_over_time,
        air_temperature_min_over_time,
        air_temperature_max_over_time,
        precipitation
    ) VALUES (
        %(ri_id)s,
        %(districtName)s,
        %(subdistrictName)s,
        %(observationDateTime)s,
        %(windSpeed_minOverTime)s,
        %(windSpeed_maxOverTime)s,
        %(relativeHumidity_minOverTime)s,
        %(relativeHumidity_maxOverTime)s,
        %(airTemperature_minOverTime)s,
        %(airTemperature_maxOverTime)s,
        %(precipitation)s
    )
"""


def callback(ch, method, properties, body):
    print(f"Received message: {body}")
    # Process the message here
    # Acknowledge the message if auto_ack is set to False
    
    data = json.loads(body)
    cursor.execute(insert_query, data)

    # Commit the transaction
    psql_connection.commit()
    print("Data inserted successfully.")
    ch.basic_ack(delivery_tag=method.delivery_tag)













def consume():
    try:
        connection_parameters = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            virtual_host='/',
            credentials=pika.PlainCredentials(
                rabbitmq_user, 
                rabbitmq_password
                )
        )
        
        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()

        # Declare the queue (in case it hasn't been declared yet)
        channel.queue_declare(queue=queue_name)

        # Set up the consumer
        channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print('Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Connection lost, retrying in 5 seconds... {str(e)}")
        time.sleep(5)
        consume()


if __name__ == "__main__":
    consume()
