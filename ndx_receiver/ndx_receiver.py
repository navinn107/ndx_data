import pika
import pika.exceptions
import psycopg2
import json
import configparser
import logging as log

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
        self.connection = self.channel = self.psql_connection = self.psql_cursor = None

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
            log.info(".......RABBITMQ CONNECTION ESTABLISHED.......")

            self.channel.queue_declare(queue=self.ri_uuid)
            self.channel.basic_qos(prefetch_count=1)

            log.info(".......STARTED CONSUMING.......")
            self.channel.basic_consume(queue=self.ri_uuid, on_message_callback=self.process_message)
            self.channel.start_consuming()

        except (
            pika.exceptions.AMQPConnectionError, 
            pika.exceptions.AMQPError, 
            pika.exceptions.AMQPHeartbeatTimeout,
            pika.exceptions.ChannelClosedByBroker,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.ConnectionClosedByClient,
            pika.exceptions.IncompatibleProtocolError,
            pika.exceptions.InvalidChannelNumber,
            pika.exceptions.NackError,
            pika.exceptions.ProbableAuthenticationError,
            pika.exceptions.ProbableAccessDeniedError,
            pika.exceptions.StreamLostError,
            pika.exceptions.UnroutableError
        ) as e:
            log.error(f"Error connecting to RabbitMQ: {e}")
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
        
        except (
            psycopg2.OperationalError, 
            psycopg2.InterfaceError
        ) as e:
            log.error(f"Error connecting to PostgreSQL: {e}")
            self.connect_postgresql()

    def process_message(self, ch, method, properties, body):
        
        """Processes incoming messages and inserts data into PostgreSQL."""
        
        try:
            log.info(".......RECEIVED MESSAGE.......")
            self.data = json.loads(body)
            self.insert_data()
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log.error(f"Database connection error: {e}")
            self.connect_postgresql()  # Reconnect if connection issue
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except (psycopg2.DatabaseError, psycopg2.IntegrityError) as e:
            log.error(f"SQL execution error: {e}")
            self.psql_connection.rollback()  # Rollback in case of failure
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            log.error(f".......ERROR PROCESSING MESSAGE: {e}.......")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def insert_data(self):
        
        """Insert data into PostgreSQL."""
        
        try:
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
            self.psql_cursor.execute(insert_query, self.data)
            self.psql_connection.commit()
            log.info(".......DATA INSERTED SUCCESSFULLY.......")
       
        except (psycopg2.DatabaseError, psycopg2.IntegrityError) as e:
            log.error(f"Error inserting data: {e}")
            self.psql_connection.rollback()

if __name__ == "__main__":
    
    ri_uuid = "784b5f80-eef8-4a9e-bc35-8ce7f940007d"

    config = configparser.ConfigParser()
    config.read("config.ini")
    
    log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    receiver = ndxReceiver(config, ri_uuid)
    receiver.connect_postgresql()
    receiver.connect_rabbitmq()
