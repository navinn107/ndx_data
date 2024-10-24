from flask import Flask, request, jsonify, send_from_directory
from flasgger import Swagger
from datetime import datetime, timedelta
import sqlite3
import hashlib
import jwt
import boto3
import configparser
import json
import psycopg2
import logging as log
import pika
import uuid

config = configparser.ConfigParser()
config.read('config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPI:

    def __init__(self, config):
        self.timeout = int(config['default']['timeout'])

        # JWT payload config
        self.algo = config["jwt_payload"]['algo']
        self.iss = config["jwt_payload"]['iss']
        self.aud = config["jwt_payload"]['aud']
        self.exp_minutes = int(config["jwt_payload"]['exp_minutes'])
        self.jti_prefix = config["jwt_payload"]['jti_prefix']
        self.role = config["jwt_payload"]['role']
        self.user = config["jwt_payload"]['user']
        self.drl = config["jwt_payload"]['drl']
        self.secretKey = config['jwt_payload']['secret']

        # PostgreSQL config
        self.psql_dbname = config["postgresql"]["dbname"]
        self.psql_user = config["postgresql"]["user"]
        self.psql_password = config["postgresql"]["password"]
        self.psql_host = config["postgresql"]["host"]
        self.psql_port = config["postgresql"]["port"]

        self.rabbitmq_user = config['RabbitMQ']['rabbitmq_user']
        self.rabbitmq_password = config['RabbitMQ']['rabbitmq_password']
        self.rabbitmq_port = int(config['RabbitMQ']['rabbitmq_port'])
        self.rabbitmq_host = config['RabbitMQ']['rabbitmq_host']
        self.exchange = config['RabbitMQ']['exchange_name']
        self.exchange_type = config['RabbitMQ']['exchange_type_name']
        self.routing = config['RabbitMQ']['routing']
        self.queue = config['RabbitMQ']['queue']

        self.connection = None
        self.channel = None

        self.connect()

        self.psql_connection = None
        self.psql_cursor = None
        
        self.app = Flask(__name__)
        self.swagger = Swagger(self.app)
        self.setup_routes()

    def connect(self):
        """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
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
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing)

        self.reply_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue

        log.info(".......RabbitMQ Connection Established.......")
        
        self.channel.basic_consume(queue=self.reply_queue, on_message_callback=self.on_reply_message_received, auto_ack=True)

    def on_reply_message_received(self, ch, method, properties, body):
        if self.correlation_id == properties.correlation_id:
            self.response_json = json.loads(body)

    def basic_publish(self, message):
        self.channel.basic_publish(exchange=self.exchange, routing_key=self.routing,
            properties=pika.BasicProperties(
                reply_to=self.reply_queue,
                correlation_id=self.correlation_id,
            ),
            body=message
        )

    def publish(self, message):
        """PUBLISHES A MESSAGE TO THE RABBITMQ QUEUE AND WAITS FOR A RESPONSE."""
        self.response_json = None
        self.correlation_id = str(uuid.uuid4())

        try:
            # Check if connection/channel is closed and reconnect if needed
            if self.connection.is_closed or not self.channel or self.channel.is_closed:
                log.info(".......CONNECTION OR CHANNEL IS CLOSED, RECONNECTING.......")
                self.connect()

            # Try to publish the message
            self.basic_publish(message)
        
        except (pika.exceptions.StreamLostError, pika.exceptions.AMQPHeartbeatTimeout) as e:
            log.error(f".......STREAM LOST OR HEARTBEAT TIMEOUT ERROR: {e}....... RECONNECTING")
            self.connect()
            log.info(".......RETRYING TO PUBLISH THE MESSAGE AFTER RECONNECTING.......")
            self.basic_publish(message)

        except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosedByBroker, pika.exceptions.AMQPChannelError) as e:
            log.error(f".......CONNECTION CLOSED OR CHANNEL ERROR: {e}....... RECONNECTING")
            self.connect()
            log.info(".......RETRYING TO PUBLISH THE MESSAGE AFTER RECONNECTING.......")
            self.basic_publish(message)

        except Exception as e:
            log.error(f".......UNEXPECTED ERROR: {e}....... RECONNECTING")
            self.connect()
            log.info(".......RETRYING TO PUBLISH THE MESSAGE AFTER RECONNECTING.......")
            self.basic_publish(message)

        # Process the response within the timeout
        self.connection.process_data_events(time_limit=self.timeout)

        if self.response_json is None:
            log.warning(f".......NO RESPONSE RECEIVED WITHIN TIMEOUT OF {self.timeout} SECONDS.......")

        return self.response_json

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
        
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log.error(f"Error connecting to PostgreSQL: {e}")
            self.connect_postgresql()

    def basic_fetch_data(self, client_id):
        """Fetches client data from PostgreSQL."""
        query = f"""
            SELECT * 
            FROM public.users 
            WHERE clientID = '{client_id}';
        """
        self.psql_cursor.execute(query)
        existing_user = self.psql_cursor.fetchone()
        return existing_user

    def registration(self, client_id, password, existing_user):
        """Handles user registration."""
        if existing_user:
            return jsonify({'statusCode': 200, 'results': "Username already exists"}), 200
        
        client_secret = hashlib.sha256(password.encode()).hexdigest()
        self.psql_cursor.execute(f"""
            INSERT INTO public.users (clientID, clientSecretKey) 
            VALUES ('{client_id}', '{client_secret}');
        """)
        self.psql_connection.commit()

        return jsonify({
            "statusCode": 200,
            "message": "Registration successful",
            "results": [{"clientID": client_id, "clientSecretKey": client_secret}]
        }), 200

    def fetch_clients(self, client_id, password=None, register=False):
        """Fetches client information, with option to register a new user."""
        try:
            if not self.psql_cursor:
                self.connect_postgresql()

            existing_user = self.basic_fetch_data(client_id)
            
            if register:
                return self.registration(client_id, password, existing_user)

        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            log.error("......BROKEN PIPE.......")
            self.connect_postgresql()
            existing_user = self.basic_fetch_data(client_id)

            if register:
                return self.registration(client_id, password, existing_user)

        except Exception as e:
            log.error(f".......ERROR FETCHING DATA....... Error: {e}")
            return {"statusCode": 500, "message": "Error fetching data"}

        return existing_user

    def generate_token(self, client_id):
        current_time = datetime.now()
        expiration_time = current_time + timedelta(minutes=int(self.exp_minutes))

        payload = {
            'sub': client_id,
            'iss': self.iss,
            'aud': self.aud,
            'exp': expiration_time.timestamp(),
            'iat': current_time.timestamp(),
            'jti': f"{self.jti_prefix}:{client_id}",
            'role': self.role,
            'cons': {},
            'user': self.user,
            'drl': self.drl
        }
        token = jwt.encode(payload, self.secretKey, algorithm=self.algo)
        return token

    def get_token(self, client_id, client_secret):
        existing_user = self.fetch_clients(client_id)

        if existing_user:
            if client_secret == existing_user[2]:
                token = self.generate_token(client_id)
                return jsonify({'token': token}), 200
            else:
                return jsonify({"statusCode": 200, 'results': "ClientID or ClientSecretKey is incorrect"}), 200
        else:
            return jsonify({"statusCode": 200, 'results': "User is not registered"}), 200

    def get_data(self):
        # Check for the JWT token in the Authorization header
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({"statusCode": 401, "detail": "Token is missing"}), 401

        msisdn_val = request.args.get('msisdn')
        if not msisdn_val:
            return jsonify({"statusCode": 400, "detail": "MSISDN VALUE IS MISSING"}), 400

        message = json.dumps({'msisdn': msisdn_val})
        response = self.publish(message)

        if response is None:
            return jsonify({"statusCode": 500, "detail": "INTERNAL SERVER ERROR"}), 500
        
        return jsonify(response), response["statusCode"]

    def setup_routes(self):
        """Sets up the Flask routes."""
        @self.app.route('/data', methods=['GET'])
        def data():
            return self.get_data()

        @self.app.route('/token', methods=['POST'])
        def token():
            data = request.get_json()
            client_id = data.get('client_id')
            client_secret = data.get('client_secret')
            return self.get_token(client_id, client_secret)

        @self.app.route('/fetch_clients', methods=['GET'])
        def fetch_clients():
            client_id = request.args.get('client_id')
            client_secret = request.args.get('client_secret')
            register = request.args.get('register', default=False, type=bool)
            return self.fetch_clients(client_id, client_secret, register)

    def run(self):
        """Runs the Flask app."""
        self.app.run(debug=True)

if __name__ == "__main__":
    rest_api = RestAPI(config)
    rest_api.run()
