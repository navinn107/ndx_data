import pika
import json
import logging as log
import redshift_connector
import ssl
import configparser

config = configparser.ConfigParser()
config.read('rabbitmq_config.ini')
log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RabbitmqServer:
    def __init__(self, rabbitmq_user, rabbitmq_password, rabbitmq_broker_id, region, rabbitmq_port, cipher_text, queue_name, redshift_host, redshift_dbname, redshift_user, redshift_password, redshift_port):
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_password = rabbitmq_password
        self.rabbitmq_broker_id = rabbitmq_broker_id
        self.region = region
        self.rabbitmq_port = rabbitmq_port
        self.cipher_text = cipher_text

        self.queue = queue_name
        self.redshift_host = redshift_host
        self.redshift_dbname = redshift_dbname
        self.redshift_user = redshift_user
        self.redshift_password = redshift_password
        self.redshift_port = redshift_port

        self.connection = None
        self.channel = None
        self.cursor = None

        self.connect()
        self.purge_queue()
        self.connect_db()

    def connect(self):
        """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
        try:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers(self.cipher_text)

            url = f"amqps://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_broker_id}.mq.{self.region}.amazonaws.com:{self.rabbitmq_port}"
            parameters = pika.URLParameters(url)
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)
 
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            self.channel.queue_declare(queue=self.queue)
            log.info(".......RABBITMQ CONNECTED.......")

        except pika.exceptions.AMQPConnectionError as e:
            log.error(f".......FAILED TO CONNECT TO RABBITMQ: {e}.......")

        except Exception as e:
            log.error(f".......UNEXPECTED ERROR DURING RABBITMQ CONNECTION: {e}.......")

    def purge_queue(self):
        """Purges the messages in the queue."""
        try:
            self.channel.queue_purge(queue=self.queue)
            log.info(".......QUEUE PURGED.......")

        except Exception as e:
            log.error(f".......FAILED TO PURGE QUEUE: {e}.......")

    def connect_db(self):
        """Establishes connection to the Redshift database."""
        try:
            self.conn_params = {
                'host': self.redshift_host,
                'database': self.redshift_dbname,
                'user': self.redshift_user,
                'password': self.redshift_password,
                'port': self.redshift_port
            }
            self.conn = redshift_connector.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            log.info(".......REDSHIFT DATABASE CONNECTED.......")

        except (redshift_connector.InterfaceError, redshift_connector.OperationalError):
            log.error(f".......FAILED TO CONNECT TO REDSHIFT: {e}.......")
        
        except Exception as e:
            log.error(f".......UNEXPECTED ERROR DURING REDSHIFT CONNECTION: {e}.......")

    def start_consume(self):
        """Starts consuming messages from RabbitMQ."""
        
        if self.connection.is_closed or self.channel.is_closed:
            log.error(".......RABBITMQ CONNECTION IS NOT AVAILABLE.......")
            self.connect()

        log.info(".......AWAITING RPC REQUESTS.......")
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.process_request)
        self.channel.start_consuming()

    def publish(self, message, reply_to, corr_id):
        """Publishes a message to RabbitMQ."""
        
        self.channel.basic_publish(
            exchange='',
            routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=corr_id),
            body=json.dumps(message)
        )
        
        log.info(f".......PUBLISHED MESSAGE: {message}.......")

    def process_request(self, ch, method, properties, body):
        """Processes incoming request messages."""
        
        try:
        
            log.info(".......RECEIVED REQUEST.......")
            request_json = json.loads(body)
            msisdn_value = request_json.get("msisdn")

            if msisdn_value:
                response = self.fetch_data(msisdn_value)
                self.publish(response, properties.reply_to, properties.correlation_id)
            else:
                response = {"statusCode": 400, "message": "MSISDN value is missing"}
                self.publish(response, properties.reply_to, properties.correlation_id)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        except Exception as e:
        
            log.error(f".......ERROR PROCESSING REQUEST: {e}.......")
            response = {"statusCode": 500, "message": "INTERNAL SERVER ERROR"}
            self.publish(response, properties.reply_to, properties.correlation_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def basic_fetch_data(self, msisdn_value):
        
        query = f'''
        
            SELECT 
                SUBSCRIBER_PERSONAL_ID, 
                SUBSCRIBER_FIRST_NAME, 
                SUBSCRIBER_LAST_NAME, 
                SUBSCRIBER_DOB,
                DATE_OF_REGISTRATION,
                MSISDN
            FROM 
                public.customers 
            WHERE 
                MSISDN = '{msisdn_value}';
        '''
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return self.fetch_response(results)

    def fetch_data(self, msisdn_value):
        """Fetches data from Redshift based on MSISDN value."""
        try:
            return self.basic_fetch_data(msisdn_value)
        except (redshift_connector.InterfaceError, redshift_connector.OperationalError):
            self.connect_db()
            return self.basic_fetch_data(msisdn_value)
        except Exception as e:
            log.error(f".......ERROR FETCHING DATA: {e}.......")
            return {"statusCode": 500, "message": "Error fetching data"}

    def fetch_response(self, results):
        """Formats fetched data into response format."""
        if results:
            data = {
                "SUBSCRIBER_PERSONAL_ID": results[0][0],
                "SUBSCRIBER_FIRST_NAME": results[0][1],
                "SUBSCRIBER_LAST_NAME": results[0][2],
                "SUBSCRIBER_DOB": results[0][3].isoformat(),
                "DATE_OF_REGISTRATION": results[0][4].isoformat(),
                "MSISDN": results[0][5]
            }
            return {'statusCode': 200, "results": data}
        else:
            return {'statusCode': 204, "message": "No content"}

if __name__ == '__main__':
    timeout = config['default']['timeout']
    rabbitmq_user = config['rabbitmq']['user']
    rabbitmq_password = config['rabbitmq']['password']
    rabbitmq_broker_id = config['rabbitmq']['broker_id']
    rabbitmq_port = config['rabbitmq']['port']
    rabbitmq_region = config['rabbitmq']['region']
    cipher_text = config['rabbitmq']['cipher_text']
    
    queue_name = config['queue']['name']
    redshift_host = config['redshift']['host']
    redshift_dbname = config['redshift']['dbname']
    redshift_user = config['redshift']['user']
    redshift_password = config['redshift']['password']
    redshift_port = config['redshift']['port']

    # Create and start the RabbitmqServer instance
    server = RabbitmqServer(
        rabbitmq_user, rabbitmq_password, rabbitmq_broker_id, rabbitmq_region,
        rabbitmq_port, cipher_text, queue_name, redshift_host, redshift_dbname,
        redshift_user, redshift_password, redshift_port
    )
    server.start_consume()
