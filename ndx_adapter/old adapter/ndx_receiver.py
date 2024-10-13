import pika
import psycopg2
import json
import configparser
import logging as log
import time

# Load configurations
config = configparser.ConfigParser()
config.read("config.ini")

ri_uuid = "784b5f80-eef8-4a9e-bc35-8ce7f940007d"

# Logging setup
log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class ndxReceiver:

    def __init__(self, config, ri_uuid):
        
        """Initialize RabbitMQ and PostgreSQL connections using the config file."""
        self.ri_uuid = ri_uuid
        
        # RabbitMQ configuration
        self.rabbitmq_user = config['RabbitMQ']['rabbitmq_user']
        self.rabbitmq_password = config['RabbitMQ']['rabbitmq_password']
        self.rabbitmq_port = int(config['RabbitMQ']['rabbitmq_port'])
        self.rabbitmq_host = config['RabbitMQ']['rabbitmq_host']

        # PostgreSQL configuration
        self.psql_dbname = config["postgresql"]["dbname"]
        self.psql_user = config["postgresql"]["user"]
        self.psql_password = config["postgresql"]["password"]
        self.psql_host = config["postgresql"]["host"]
        self.psql_port = config["postgresql"]["port"]

        # Initialize connections
        self.connection = None
        self.channel = None
        self.psql_connection = None
        self.psql_cursor = None

    def connect_rabbitmq(self):
        """Establishes connection to RabbitMQ and declares queue."""
        
        try:
        
            connection_params = pika.ConnectionParameters(
                host=self.rabbitmq_host,
                port=self.rabbitmq_port,
                virtual_host='/',
                credentials=pika.PlainCredentials(
                    self.rabbitmq_user, 
                    self.rabbitmq_password
                )
            )
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()

            # Declare queue
            self.channel.queue_declare(queue=self.ri_uuid)

            log.info(".......RABBITMQ CONNECTION ESTABLISHED.......")

            self.start_consuming()
        
        except pika.exceptions.AMQPConnectionError as e:
            log.error(f".......FAILED TO CONNECT TO RABBITMQ: {e}.......")
            self.retry_rabbitmq_connection()

    def retry_rabbitmq_connection(self):
        """Retries RabbitMQ connection in case of failure."""
        log.info(".......RETRYING RABBITMQ CONNECTION.......")
        time.sleep(5)
        self.connect_rabbitmq()

    def connect_postgresql(self):
        """Connects to PostgreSQL database."""
        try:
            self.psql_connection = psycopg2.connect(
                dbname=self.psql_dbname,
                user=self.psql_user,
                password=self.psql_password,
                host=self.psql_host,
                port=self.psql_port
            )
            self.psql_cursor = self.psql_connection.cursor()
            log.info(".......POSTGRESQL CONNECTION ESTABLISHED.......")
        
        except psycopg2.Error as e:
            log.error(f".......ERROR CONNECTING TO POSTGRESQL: {e}.......")

    def start_consuming(self):
        """Starts consuming messages from RabbitMQ."""
        try:
        
            if self.connection.is_closed or self.channel.is_closed:
                log.error(".......RABBITMQ CONNECTION IS NOT AVAILABLE.......")
                self.connect_rabbitmq()

            log.info(".......AWAITING MESSAGES.......")
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.ri_uuid, on_message_callback=self.process_message)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            log.error(".......RABBITMQ CONNECTION LOST: {e}.......")
            self.retry_rabbitmq_connection()

    def process_message(self, ch, method, properties, body):
        """Processes incoming messages and inserts data into PostgreSQL."""
        try:
            log.info(".......RECEIVED MESSAGE.......")
            data = json.loads(body)

            # SQL query for inserting data
            insert_query = """
                INSERT INTO weather_data(
                    ri_id,
                    district_name,
                    subdistrict_name,
                    observation_datetime,
                    wind_speed_min_over_time,
                    wind_speed_max_over_time,
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
            # Execute query and commit changes
            self.psql_cursor.execute(insert_query, data)
            self.psql_connection.commit()

            log.info(".......DATA INSERTED SUCCESSFULLY.......")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            log.error(f".......ERROR PROCESSING MESSAGE: {e}.......")
            ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == "__main__":
    receiver = ndxReceiver(config, ri_uuid)
    receiver.connect_rabbitmq()
    receiver.connect_postgresql()
