from flask import Flask, request, jsonify, send_from_directory
from flasgger import Swagger
from datetime import datetime, timedelta
import configparser
import hashlib
import jwt
import psycopg2
import logging as log
import pika
import uuid
import json

config = configparser.ConfigParser()
config.read('config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPI:

    def __init__(self, config):
        self._initialize_config(config)
        self._initialize_connections()
        self.app = Flask(__name__)
        self.swagger = Swagger(self.app)
        self.setup_routes()

    def _initialize_config(self, config):
        # General Config
        self.timeout = int(config['default']['timeout'])

        # JWT Config
        jwt_payload = config['jwt_payload']
        self.algo = jwt_payload['algo']
        self.iss = jwt_payload['iss']
        self.aud = jwt_payload['aud']
        self.exp_minutes = int(jwt_payload['exp_minutes'])
        self.jti_prefix = jwt_payload['jti_prefix']
        self.role = jwt_payload['role']
        self.user = jwt_payload['user']
        self.drl = jwt_payload['drl']
        self.secretKey = jwt_payload['secret']

        # PostgreSQL Config
        postgres = config['postgresql']
        self.psql_dbname = postgres['dbname']
        self.psql_user = postgres['user']
        self.psql_password = postgres['password']
        self.psql_host = postgres['host']
        self.psql_port = postgres['port']

        # RabbitMQ Config
        rabbitmq = config['RabbitMQ']
        self.rabbitmq_user = rabbitmq['rabbitmq_user']
        self.rabbitmq_password = rabbitmq['rabbitmq_password']
        self.rabbitmq_port = int(rabbitmq['rabbitmq_port'])
        self.rabbitmq_host = rabbitmq['rabbitmq_host']
        self.exchange = rabbitmq['exchange_name']
        self.exchange_type = rabbitmq['exchange_type_name']
        self.routing = rabbitmq['routing']
        self.queue = rabbitmq['queue']

    def _initialize_connections(self):
        self.connection = None
        self.channel = None
        self.psql_connection = None
        self.psql_cursor = None
        self.connect_rabbitmq()
        self.connect_postgresql()

    def connect_rabbitmq(self):
        connection_params = pika.ConnectionParameters(
            host=self.rabbitmq_host,
            port=self.rabbitmq_port,
            credentials=pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_password)
        )
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()
        self._setup_rabbitmq_exchange_queue()
        log.info("RabbitMQ Connection Established")

    def _setup_rabbitmq_exchange_queue(self):
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing)
        self.reply_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.basic_consume(queue=self.reply_queue, on_message_callback=self.on_reply_message_received, auto_ack=True)

    def connect_postgresql(self):
        try:
            self.psql_connection = psycopg2.connect(
                dbname=self.psql_dbname,
                user=self.psql_user,
                password=self.psql_password,
                host=self.psql_host,
                port=self.psql_port
            )
            self.psql_cursor = self.psql_connection.cursor()
            log.info("PostgreSQL Connection Established")
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log.error(f"Error connecting to PostgreSQL: {e}")
            self.connect_postgresql()

    def fetch_clients(self, client_id):
        if not self.psql_cursor:
            self.connect_postgresql()
        query = f"SELECT * FROM public.users WHERE clientID = '{client_id}';"
        self.psql_cursor.execute(query)
        return self.psql_cursor.fetchone()

    def registration(self):
        if request.headers.get('Content-Type') != 'application/json':
            return jsonify({'error': 'Unsupported Media Type', 'message': 'Request must be in JSON format'}), 415

        data = request.json
        username, password, secretKey = data.get('username'), data.get('password'), data.get('secretKey')

        if not all([username, password, secretKey]):
            return jsonify({"message": "username, password and secretKey are required."}), 400
        
        if secretKey != self.secretKey:
            return jsonify({"message": "secretKey is wrong. Please, try again."}), 400

        client_id = hashlib.sha256(username.encode()).hexdigest()
        existing_user = self.fetch_clients(client_id)

        if not existing_user:
            client_secret = hashlib.sha256(password.encode()).hexdigest()
            self.psql_cursor.execute(f"INSERT INTO public.users (clientID, clientSecretKey) VALUES ('{client_id}', '{client_secret}');")
            self.psql_connection.commit()
            return jsonify({"statusCode": 200, "message": "Registration successful", "results": [{"clientID": client_id, "clientSecretKey": client_secret}]}), 200
        return jsonify({'statusCode': 200, 'results': "Username already exists"}), 200

    def get_token(self):
        data = request.json
        client_id, client_secret = data.get('clientID'), data.get('clientSecretKey')

        if not all([client_id, client_secret]):
            return jsonify({"statusCode": 400, "detail": "Both clientID and clientSecretKey are required."}), 400

        existing_user = self.fetch_clients(client_id)
        if existing_user and client_secret == existing_user[2]:
            current_time = datetime.now()
            expiration_time = current_time + timedelta(minutes=self.exp_minutes)
            payload = {
                'sub': client_id, 'iss': self.iss, 'aud': self.aud, 'exp': expiration_time.timestamp(),
                'iat': current_time.timestamp(), 'jti': f"{self.jti_prefix}:{client_id}", 'role': self.role,
                'cons': {}, 'user': self.user, 'drl': self.drl
            }
            token = jwt.encode(payload, self.secretKey, algorithm=self.algo)
            return jsonify({'token': token}), 200
        return jsonify({"statusCode": 200, 'results': "ClientID or ClientSecretKey is incorrect"}), 200

    def on_reply_message_received(self, ch, method, properties, body):
        if self.correlation_id == properties.correlation_id:
            self.response_json = json.loads(body)

    def basic_publish(self, message):
        self.channel.basic_publish(
            exchange=self.exchange, routing_key=self.routing,
            properties=pika.BasicProperties(reply_to=self.reply_queue, correlation_id=self.correlation_id),
            body=message
        )

    def publish(self, message):
        self.response_json = None
        self.correlation_id = str(uuid.uuid4())
        try:
            if not self.channel:
                self.connect_rabbitmq()
            self.basic_publish(message)
        except (pika.exceptions.StreamLostError, pika.exceptions.AMQPHeartbeatTimeout):
            log.info("Stream Lost or Channel Closed, Reconnecting...")
            self.connect_rabbitmq()
            self.basic_publish(message)
        return self.response_json

    def get_data(self):
        token = request.headers.get("token")
        if not token:
            return jsonify({"statusCode": 400, 'detail': 'Token is required'}), 400

        decoded_payload = jwt.decode(token, options={"verify_signature": False}, algorithms=[self.algo])
        client_id = decoded_payload["sub"]

        existing_user = self.fetch_clients(client_id)
        if not existing_user:
            return jsonify({"statusCode": 401, 'detail': 'User not found'}), 401

        if datetime.now() > datetime.fromtimestamp(decoded_payload['exp']):
            return jsonify({"statusCode": 401, 'detail': 'Token expired'}), 401

        uuid = request.args.get('uuid')
        if not uuid:
            return jsonify({"statusCode": 400, "detail": "ID IS MISSING"}), 400

        data = {
            'id': uuid,
            'districtName': request.args.get('district_name'),
            'subdistrictName': request.args.get('subdistrict_name'),
            'observation_datetime': request.args.get('observation_datetime')
        }
        response = self.publish(json.dumps(data))
        if response:
            return jsonify(response), response["statusCode"]
        return jsonify({"statusCode": 500, "detail": "INTERNAL SERVER ERROR"}), 500

    def setup_routes(self):
        @self.app.route("/api/ndx/register", methods=['POST'])
        def register_route():
            return self.registration()

        @self.app.route('/api/ndx/get-token', methods=['POST'])
        def get_token_route():
            return self.get_token()

        @self.app.route('/api/ndx/get-data', methods=['POST'])
        def get_data_route():
            return self.get_data()

        @self.app.route("/", methods=['GET'])
        def documentation():
            return send_from_directory('static', 'index.html')

        @self.app.errorhandler(405)
        def method_not_allowed(e):
            return jsonify({'error': 'Method Not Allowed', 'message': 'The method is not allowed for the requested URL.'}), 405

        @self.app.errorhandler(404)
        def not_found(e):
            return jsonify({'error': 'Not Found', 'message': 'The requested URL was not found on the server.'}), 404


if __name__ == "__main__":
    rest_api = RestAPI(config)
    rest_api.app.run(host='0.0.0.0', port=5000)
