from flask import Flask, request, jsonify, send_from_directory
from flasgger import Swagger
from datetime import datetime, timedelta
import hashlib
import jwt
import configparser
import json
import psycopg2
import logging as log
import pika
import uuid
import time
from jwt.exceptions import DecodeError

config = configparser.ConfigParser()
config.read('config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPI:

    def __init__(self, config):

        self.timeout = int(config['default']['timeout'])
     
        self.algo = config["jwt_payload"]['algo']
        self.iss = config["jwt_payload"]['iss']
        self.aud = config["jwt_payload"]['aud']
        self.exp_minutes = int(config["jwt_payload"]['exp_minutes'])
        self.jti_prefix = config["jwt_payload"]['jti_prefix']
        self.role = config["jwt_payload"]['role']
        self.user = config["jwt_payload"]['user']
        self.drl = config["jwt_payload"]['drl']
        self.secretKey = config['jwt_payload']['secret']
        
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
        self.psql_connection = None
        self.psql_cursor = None

        self.app = Flask(__name__)
        self.swagger = Swagger(self.app)
        self.setup_routes()

    def connect(self):
      
      """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
      attempts = 0
      max_retries=5
      retry_delay=5

      while attempts < max_retries:
        
        try:
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
          log.info(".......RabbitMQ Connection Established.......")

          self.channel = self.connection.channel()

          self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
          self.channel.queue_declare(queue=self.queue)
          self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing)
          self.reply_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
          self.channel.basic_consume(queue=self.reply_queue, on_message_callback=self.on_reply_message_received, auto_ack=True)

          break

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
            
              attempts += 1
              log.error(f"Error connecting to Rabbitmq (Attempt {attempts}/{max_retries}): {e}")
              
              if attempts < max_retries:
                  log.info(f"Retrying in {retry_delay} seconds...")
                  time.sleep(retry_delay)  # Wait before retrying
              else:
                  log.error("Maximum retry attempts reached. Could not establish Rabbitmq connection.")

    def connect_postgresql(self, ):
      
      """Connects to PostgreSQL database with a retry mechanism."""
      attempts = 0
      max_retries=5
      retry_delay=5

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
          log.info(".......POSTGRESQL CONNECTION ESTABLISHED.......")
          break

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
          attempts += 1
          log.error(f"Error connecting to PostgreSQL (Attempt {attempts}/{max_retries}): {e}")
          
          if attempts < max_retries:
              log.info(f"Retrying in {retry_delay} seconds...")
              time.sleep(retry_delay)  # Wait before retrying
          else:
              log.error("Maximum retry attempts reached. Could not establish PostgreSQL connection.")
  
    def fetch_clients(self, client_id):
        
        """Fetches client data from PostgreSQL."""

        attempts = 0
        max_retries = 5
        retry_delay = 5  # in seconds
        
        while attempts < max_retries:
            try:
                if not self.psql_cursor:
                    self.connect_postgresql()
                
                query = f"""SELECT * FROM public.users WHERE clientID = '{client_id}';"""
                self.psql_cursor.execute(query)
                existing_user = self.psql_cursor.fetchone()
                return existing_user
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                attempts += 1
                log.error(f"Attempt {attempts} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)                
                self.connect_postgresql()
            
            except psycopg2.Error as e:
                log.error(f"PostgreSQL error occurred: {str(e)}")
                break

        return None

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
        self.response_json = None
        self.correlation_id = str(uuid.uuid4())

        try:
            if not self.channel:
              self.connect()

            self.basic_publish(message)

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
            
            log.info(".......STREAM LOST THE CONNECTION OR CHANNEL IS CLOSED, RECONNECTING.......")
            self.connect()
            self.basic_publish(message)

        self.connection.process_data_events(time_limit=self.timeout)

        return self.response_json
           
    def registration(self):
        
        if request.headers.get('Content-Type') != 'application/json':
            return jsonify({'error': 'Unsupported Media Type', 'message': 'Request must be in JSON format'}), 415

        data = request.json
        username = data.get('username')
        password = data.get('password')
        secretKey = data.get('secretKey')

        if not username or not password or not secretKey:
            return jsonify({"message": "username, password and secretKey are required."}), 400
        
        if secretKey!=self.secretKey:
            return jsonify({"message": "secretKey is wrong. Please, try again."}), 400

        client_id = hashlib.sha256(username.encode()).hexdigest()
        existing_user = self.fetch_clients(client_id)
        
        if not existing_user:
            client_secret = hashlib.sha256(password.encode()).hexdigest()
            self.psql_cursor.execute(f"""INSERT INTO public.users (clientID, clientSecretKey) VALUES ('{client_id}', '{client_secret}');""")
            self.psql_connection.commit()
            return jsonify(
                {
                    "statusCode": 200,
                    "message": "Registration successful",
                    "results": [{"clientID": client_id, "clientSecretKey": client_secret}]
                }
            ), 200
        else:
            return jsonify({'statusCode': 200, 'results': "Username already exists"}), 200

    def get_token(self):

        data = request.json
        client_id = data.get('clientID')
        client_secret = data.get('clientSecretKey')
        
        if not client_id or not client_secret:
            return jsonify({"statusCode": 400, "detail": "Both clientID and clientSecretKey are required."}), 400
        
        existing_user = self.fetch_clients(client_id)

        if existing_user:
            
            if client_secret == existing_user[2]:
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
                return jsonify({'token': token}), 200
            
            else:
                return jsonify({"statusCode": 200,'results': "ClientID or ClientSecretKey is incorrect"}), 200
        
        else:
            return jsonify({"statusCode": 200,'results': "User is not registered"}), 200
 
    def get_data(self):

        token = request.headers.get("token")

        if not token:
            return jsonify({"statusCode": 400, 'detail': 'Token is required'}), 400
        
        try:
          decoded_payload = jwt.decode(token, options={"verify_signature": False}, algorithms=[self.algo])
          
        except DecodeError:
          return jsonify({"statusCode": 401, 'detail': 'Invalid token'}), 401

  
        client_id = decoded_payload["sub"]
        existing_user = self.fetch_clients(client_id)
        if not existing_user:
            return jsonify({"statusCode": 401, 'detail': 'User not found'}), 401

        if datetime.now() > datetime.fromtimestamp(decoded_payload['exp']):
            return jsonify({"statusCode": 401, 'detail': 'Token expired'}), 401
        
        uuid = request.args.get('uuid')
        if not uuid:
                return jsonify({"statusCode": 400, "detail": "UUID IS MISSING"}), 400
    

        districtName = request.args.get('district_name')
        subdistrictName = request.args.get('subdistrict_name')
        observation_datetime = request.args.get('observation_datetime')


        data = {
            'ri_id': uuid,
            'district_name': districtName, 
            'subdistrict_name': subdistrictName, 
            'observation_datetime': observation_datetime
            }
        
        message = json.dumps(data)

        response = self.publish(message)

        if response:
            log.info('.................RESPONSE DONE..............')

            return jsonify(response), response["statusCode"]
        else:
            log.info('.................NO RESPONSE MADE..............')
            return jsonify({"statusCode": 500, "detail": "INTERNAL SERVER ERROR"}), 500
 
    def setup_routes(self):
        
        """Setup Flask routes."""
        @self.app.route("/api/ndx/register", methods=['POST'])
        def register_route():
            """
            Register a new user
            ---
            tags:
              - User Registration
            parameters:
              - in: header
                name: Content-Type
                type: string
                required: true
                default: application/json
              - in: body
                name: body
                required: true
                schema:
                  type: object
                  required:
                    - username
                    - password
                    - secretKey
                  properties:
                    username:
                      type: string
                    password:
                      type: string
                    secretKey:
                      type: string
            responses:
              200:
                description: Registration successful
                schema:
                  type: object
                  properties:
                    message:
                      type: string
                    results:
                      type: array
                      items:
                        type: object
                        properties:
                          clientID:
                            type: string
                          clientSecretKey:
                            type: string
                    
              400:
                description: Bad request
              415:
                description: Unsupported Media Type
            """
            return self.registration()

        @self.app.route('/api/ndx/get-token', methods=['POST'])
        def get_token_route():
            """
            Get JWT token
            ---
            tags:
              - Authentication
            parameters:
              - in: body
                name: body
                required: true
                schema:
                  type: object
                  required:
                    - clientID
                    - clientSecretKey
                  properties:
                    clientID:
                      type: string
                    clientSecretKey:
                      type: string
            responses:
              200:
                description: Token generated
                schema:
                  type: object
                  properties:
                    token:
                      type: string
              400:
                description: Bad request
            """

            return self.get_token()

        @self.app.route('/api/ndx/get-data', methods=['POST'])
        def get_data_route():
            
            """
                Endpoint to fetch data based on MSISDN value.
                ---
                parameters:
                  - name: uuid
                    in: query
                    type: string
                    required: true
                    description: The ID value

                  - name: district_name
                    in: query
                    type: string
                    description: The district Name

                  - name: subdistrict_name
                    in: query
                    type: string
                    description: The subdistrict Name

                  - name: observation_datetime
                    in: query
                    type: string
                    description: The observation datetime
                
                  - in: header
                    name: token
                    type: string
                    required: true
                    description: Authorization Token


                responses:
                  200:
                    description: Successful response with data
                  400:
                    description: Missing MSISDN value
                  401:
                    description: Invalid or missing token
                  500:
                    description: No response from server
            """
            return self.get_data()
        
        @self.app.route("/", methods=['GET'])
        def documentation():
            return send_from_directory('static', 'index.html')

        @self.app.errorhandler(405)
        def method_not_allowed(e):
            return jsonify({'error': 'Method Not Allowed', 'message': 'The method is not allowed for the requested URL.'}), 405

        @self.app.errorhandler(404)
        def page_not_found(e):
            return jsonify({'error': 'Invalid URL', 'detail': 'The requested URL is not found on the server.'}), 404

my_app = RestAPI(config).app

#    my_app.run(host='0.0.0.0', port=5000, debug=True)
