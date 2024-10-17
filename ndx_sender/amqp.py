import pika
import json
import logging as log
import configparser

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class AMQP:
    def __init__(self, config):
        
        """Initialize the AMQP client and load general configurations from the config file."""

        # Load RabbitMQ connection configurations from the config file
        self.rabbitmq_user = config['RabbitMQ']['rabbitmq_user']
        self.rabbitmq_password = config['RabbitMQ']['rabbitmq_password']
        self.rabbitmq_port = int(config['RabbitMQ']['rabbitmq_port'])
        self.rabbitmq_host = config['RabbitMQ']['rabbitmq_host']
        self.exchange_type = config['RabbitMQ']['exchange_type_name']

        self.connection = None
        self.channel = None

    def connect(self, exchange, routing_key, queue):
        """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
        
        # Load exchange, queue, and routing key configurations
        self.exchange = exchange
        self.routing_key = routing_key
        self.queue = queue

        connection_parameters = pika.ConnectionParameters(
            host=self.rabbitmq_host,
            port=self.rabbitmq_port,
            virtual_host='/',
            credentials=pika.PlainCredentials(
                self.rabbitmq_user, 
                self.rabbitmq_password
                )
        )
        
        self.connection = pika.BlockingConnection(connection_parameters)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)

        log.info(".......RabbitMQ Connection Established.......")

    def publish_data(self, data):
        """Publishes the transformed data to RabbitMQ."""

        message = json.dumps(data, indent=4)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=message,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=1)
        )
        
        log.info(" [x] Sent data to RabbitMQ")



