import pika
import json
import logging as log
import configparser
import psycopg2
import time
from datetime import datetime

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Set up logging
log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPIReceiver:
    
    def __init__(self, config):
        # RabbitMQ Configuration
        self.rabbitmq_user = config['RabbitMQ']['rabbitmq_user']
        self.rabbitmq_password = config['RabbitMQ']['rabbitmq_password']
        self.rabbitmq_port = int(config['RabbitMQ']['rabbitmq_port'])
        self.rabbitmq_host = config['RabbitMQ']['rabbitmq_host']
        self.queue = config['RabbitMQ']['queue']
        
        # PostgreSQL Configuration
        self.psql_dbname = config["postgresql"]["dbname"]
        self.psql_user = config["postgresql"]["user"]
        self.psql_password = config["postgresql"]["password"]
        self.psql_host = config["postgresql"]["host"]
        self.psql_port = config["postgresql"]["port"]

        self.connection = None
        self.channel = None
        self.psql_connection = None
        self.psql_cursor = None

        # Initialize connections
        self.connect_rabbitmq()
        self.purge_queue()
        self.connect_postgresql()

    def connect_rabbitmq(self):
        """Establishes connection to RabbitMQ and sets up channel and queue."""
        attempts = 0
        max_retries = 5
        retry_delay = 5

        while attempts < max_retries:
            try:
                connection_parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    virtual_host='/',
                    credentials=pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_password)
                )
                self.connection = pika.BlockingConnection(connection_parameters)
                log.info(".......RabbitMQ Connection Established.......")
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue)
                break
            
            except (
                pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPError,
                pika.exceptions.AMQPHeartbeatTimeout, pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ConnectionClosedByBroker, pika.exceptions.ConnectionClosedByClient
            ) as e:
                
                attempts += 1
                log.error(f"Error connecting to RabbitMQ (Attempt {attempts}/{max_retries}): {e}")
                if attempts < max_retries:
                    log.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    log.error("Maximum retry attempts reached. Could not establish RabbitMQ connection.")

    def purge_queue(self):
        """Purges the messages in the queue."""
        self.channel.queue_purge(queue=self.queue)

    def connect_postgresql(self):
        """Connects to PostgreSQL database with a retry mechanism."""
        attempts = 0
        max_retries = 5
        retry_delay = 5

        while attempts < max_retries:
            try:
                self.psql_connection = psycopg2.connect(
                    dbname=self.psql_dbname,
                    user=self.psql_user,
                    password=self.psql_password,
                    host=self.psql_host,
                    port=self.psql_port
                )
                self.psql_cursor = self.psql_connection.cursor()
                log.info(".......PostgreSQL Connection Established.......")
                break
            
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            
                attempts += 1
                log.error(f"Error connecting to PostgreSQL (Attempt {attempts}/{max_retries}): {e}")
                if attempts < max_retries:
                    log.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    log.error("Maximum retry attempts reached. Could not establish PostgreSQL connection.")

    def start_consume(self):
        
        """Starts consuming messages from RabbitMQ."""
        if self.connection.is_closed or self.channel.is_closed:
            log.error(".......RabbitMQ Connection is not available.......")
            self.connect_rabbitmq()

        log.info(".......Awaiting RPC requests.......")
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.process_request)
        self.channel.start_consuming()

    def process_request(self, ch, method, properties, body):
        
        """Processes the received message and fetches data from PostgreSQL."""
        
        log.info(".......Received Request.......")
        request_json = json.loads(body)

        # Fetch data from PostgreSQL based on request JSON
        dictionary = self.fetch_data(request_json)

        # Publish the response back to RabbitMQ
        self.publish(dictionary, properties.reply_to, properties.correlation_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def fetch_data(self, json_array):
        """Fetches data from PostgreSQL based on the request."""
        attempts = 0
        max_retries = 3
        retry_delay = 3
        dictionary = {}
        valid_formats = ["<", ">", "="]  # Allowed formats for observation_datetime

        print(json_array)

        while attempts < max_retries:
            try:
                # v = " AND ".join(["{}='{}'".format(i, j) for i, j in json_array.items() if j is not None ])
                conditions = []

                # Build the query conditions
                for key, value in json_array.items():
                    if json_array.get(key, None):

                        if key == "observation_datetime":
                            # Check if the value contains a valid operator
                            if any(op in value for op in valid_formats):
                                if '>' in value:
                                    conditions.append(f"{key} > '{value.replace('>', '').strip()}'")
                                elif '<' in value:
                                    conditions.append(f"{key} < '{value.replace('<', '').strip()}'")
                                elif '=' in value:
                                    conditions.append(f"{key} = '{value.replace('=', '').strip()}'")
                                else:
                                    log.error(f"Invalid format for observation_datetime: {value}")
                                    return {"statusCode": 400, "message": "Invalid observation_datetime format. Allowed formats are 'YYYY-MM-DD', 'YYYY-MM-DD <', 'YYYY-MM-DD >'."}
                            else:
                                # Invalid if it's neither <, >, nor =
                                log.error(f"Invalid format for observation_datetime: {value}")
                                return {"statusCode": 400, "message": "Invalid observation_datetime format. Allowed formats are 'YYYY-MM-DD', 'YYYY-MM-DD <', 'YYYY-MM-DD >'."}
                        else:
                            # For all other fields, use equality
                            conditions.append(f"{key} = '{value}'")

                where_clause = " AND ".join(conditions)
                query = f"SELECT * FROM weather_data WHERE {where_clause};"
                self.psql_cursor.execute(query)
                results = self.psql_cursor.fetchall()
                return self.fetch_response(results)

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                attempts += 1
                log.error(f"Error connecting to PostgreSQL (Attempt {attempts}/{max_retries}): {e}")
                if attempts < max_retries:
                    self.connect_postgresql()
                    log.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    log.error("Maximum retry attempts reached. Could not establish PostgreSQL connection.")

    def fetch_response(self, results):
        """Fetches and formats the response from PostgreSQL."""
        dictionary = {}
        if results:
            data = []
            for result in results:

                date_string = str(result[3]) if result[3] else None

                observationDateTime = lambda date_string: datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%dT%H:%M:%S+05:30")

                packet = {
                    "id": str(result[0]) if result[0] else None,
                    "districtName": str(result[1]) if result[1] else None,
                    "subdistrictName": str(result[2]) if result[2] else None,
                    "observationDateTime": observationDateTime(date_string),
                    "precipitation": str(result[4]) if result[4] else None,
                    "air_temperature_max_over_time": str(result[5]) if result[5] else None,
                    "air_temperature_min_over_time": str(result[6]) if result[6] else None,
                    "relative_humidity_max_over_time": float(result[7]) if result[7] else None,
                    "relative_humidity_min_over_time": float(result[8]) if result[8] else None,
                    "wind_speed_max_over_time": float(result[9]) if result[9] else None,
                    "wind_speed_min_over_time": float(result[10]) if result[10] else None,
                }
                data.append(packet)

            dictionary['statusCode'] = 200
            dictionary["results"] = data
        else:
            dictionary['statusCode'] = 204
            dictionary["results"] = "No Content"

        return dictionary

    def publish(self, message, reply_to, corr_id):
        """Publishes a message to RabbitMQ."""
        self.channel.basic_publish(
            exchange='',
            routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=corr_id),
            body=json.dumps(message)
        )
        log.info(f".......Published Message: {message}.......")

# Example usage
if __name__ == "__main__":
    receiver = RestAPIReceiver(config)
    receiver.start_consume()
