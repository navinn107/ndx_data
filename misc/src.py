from flask import Flask, request, jsonify, send_from_directory
from flasgger import Swagger
from datetime import datetime, timedelta
import sqlite3
import hashlib
import jwt
import boto3
import configparser
import json
import redshift_connector
import logging as log

config = configparser.ConfigParser()
config.read('restapi_config.ini')
log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPI:

    def __init__(self, ACCESS_KEY, BUCKET_NAME, OBJECT_KEY, SECRET_KEY, algo, iss, aud, exp_minutes, jti_prefix, role, user, drl, redshift_host, redshift_dbname, redshift_user, redshift_password, redshift_port):
        
        self.app = Flask(__name__)
        self.swagger = Swagger(self.app)

        self.ACCESS_KEY = ACCESS_KEY
        self.BUCKET_NAME = BUCKET_NAME
        self.OBJECT_KEY = OBJECT_KEY
        self.SECRET_KEY = SECRET_KEY
        self.algo = algo
        self.iss = iss
        self.aud = aud
        self.exp_minutes = exp_minutes
        self.jti_prefix = jti_prefix
        self.role = role
        self.user = user
        self.drl = drl

        self.redshift_host = redshift_host
        self.redshift_dbname = redshift_dbname
        self.redshift_user = redshift_user
        self.redshift_password = redshift_password
        self.redshift_port = redshift_port

        self.cursor = None
        self.connect_db()
        self.setup_routes()
          
    def connect_db(self):
      
      """Establishes connection to the Redshift database."""
      try:
        
        self.conn_params = {
              'host': self.redshift_host,
              'database': self.redshift_dbname,
              'user': self.redshift_user,
              'password': self.redshift_password,
              'port': self.redshift_port}
        
        self.conn = redshift_connector.connect(**self.conn_params)
        self.cursor = self.conn.cursor()
        
        log.info(".......REDSHIFT DATABASE CONNECTED.......")
      
      except (redshift_connector.InterfaceError, redshift_connector.OperationalError) as e:
        log.error(f".......FAILED TO CONNECT TO REDSHIFT: {e}.......")
        
      except Exception as e:
        log.error(f".......UNEXPECTED ERROR DURING REDSHIFT CONNECTION: {e}.......")

    def basic_fetch_data(self, client_id):
      
      query = f'''        
            SELECT *
            FROM 
                public.users 
            WHERE 
                CLIENTID = '{client_id}';
      '''
      self.cursor.execute(query)
      
      existing_user = self.cursor.fetchone()
      return existing_user
        
    def registration(self, client_id, password, existing_user, register):
        
        if register:
          
          if existing_user:
            return jsonify({'statusCode': 200,'results': "Username already exists"}), 200
          
          client_secret = hashlib.sha256(password.encode()).hexdigest()
          self.cursor.execute(f"INSERT INTO public.users (clientID, clientSecretKey) VALUES ('{client_id}', '{client_secret}')")
          self.conn.commit()

          return jsonify({
                "statusCode": 200,
                "message": "Registration successful",
                "results": [{"clientID": client_id, "clientSecretKey": client_secret}]
            }), 200

    def fetch_clients(self, client_id, password=None, register=False):
      
      try:
          if not self.cursor:
              self.connect_db()
          
          existing_user = self.basic_fetch_data(client_id)
          
          if register:
              return self.registration(client_id, password, existing_user, register)

      except (redshift_connector.InterfaceError, redshift_connector.OperationalError):
        log.error(f"......BROKEN PIPE.......")
        self.connect_db()
        existing_user = self.basic_fetch_data(client_id)
          
        if register:
            return self.registration(client_id, password, existing_user, register)      
      
      except Exception as e:
        log.error(f".......ERROR FETCHING DATA.......")
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
        token = jwt.encode(payload, self.SECRET_KEY, algorithm=self.algo)
        return token

    def authenticate(self, token):
        try:
            decoded_payload = jwt.decode(token, options={"verify_signature": False}, algorithms=[self.algo])
            client_id = decoded_payload["sub"]

            if self.fetch_clients(client_id):
                current_time = datetime.now()
                token_expiration_time = datetime.fromtimestamp(decoded_payload['exp'])

                if current_time > token_expiration_time:
                    return jsonify({"statusCode": 401, 'detail': 'Token expired'}), 401
                else:
                    return self.get_data()

            else:
                return jsonify({"statusCode": 401,'detail': 'No authorization'}), 401

        except jwt.InvalidTokenError:
            return jsonify({"statusCode": 401,'detail': 'Invalid or expired token'}), 401

    def register_user(self, username, password):
        client_id = hashlib.sha256(username.encode()).hexdigest()
        return self.fetch_clients(client_id, password=password, register=True)

    def get_token(self, client_id, client_secret):
        existing_user = self.fetch_clients(client_id)

        if existing_user:
            if client_secret == existing_user[2]:
                token = self.generate_token(client_id)
                return jsonify({'token': token}), 200
            else:
                return jsonify({"statusCode": 200,'results': "ClientID or ClientSecretKey is incorrect"}), 200
        else:
            return jsonify({"statusCode": 200,'results': "User is not registered"}), 200

    def get_data(self):
        session = boto3.Session(aws_access_key_id=self.ACCESS_KEY, aws_secret_access_key=self.SECRET_KEY)
        s3 = session.client('s3')
        response = s3.get_object(Bucket=self.BUCKET_NAME, Key=self.OBJECT_KEY)

        json_str = response['Body'].read().decode('utf-8')
        json_array = json.loads(json_str)

        if request.args:
            station_code = request.args.get("station_code")
            if station_code:
                filter_data = [json_val for json_val in json_array if json_val["station_code"] == int(station_code)]
                return jsonify({"results": filter_data})
            else:
                return jsonify({"statusCode": 400,'error': 'Bad Request', 'message': 'station_code is the only filter'}), 400
        else:
            return jsonify({"statusCode": 200, "results": json_array})

    def setup_routes(self):
        @self.app.route('/api/ndx/get-data', methods=['POST'])
        def get_data_route():
            """
            Get data from S3 bucket
            ---
            tags:
              - Data Retrieval
            parameters:
              - in: header
                name: token
                type: string
                required: true
              - in: query
                name: station_code
                type: integer
                required: false
            responses:
              200:
                description: Data retrieved successfully
              400:
                description: Bad request
              401:
                description: Unauthorized
              404:
                description: Not found
            """
            authorization_token = request.headers.get("token")
            if authorization_token:
                return self.authenticate(authorization_token)
            else:
                return jsonify({'message': 'Invalid URL'}), 404

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
                    - secretKey
                  properties:
                    clientID:
                      type: string
                    clientSecretKey:
                      type: string
                    secretKey:
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
            data = request.json
            client_id = data.get('clientID')
            client_secret = data.get('clientSecretKey')
            if not client_id or not client_secret:
                return jsonify({"statusCode": 400, "detail": "Both clientID and clientSecretKey are required."}), 400
            return self.get_token(client_id, client_secret)

        @self.app.route("/api/ndx/register", methods=['POST'])
        def register_route():
            """
            Register a new user
            ---
            tags:
              - User Management
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
                  properties:
                    username:
                      type: string
                    password:
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

            
            if request.headers.get('Content-Type') != 'application/json':
                return jsonify({'error': 'Unsupported Media Type', 'message': 'Request must be in JSON format'}), 415

            # try:
              
            data = request.json
            username = data.get('username')
            password = data.get('password')

            if not username or not password:
                return jsonify({"message": "Both username and password are required."}), 400

            return self.register_user(username, password)

            # except Exception as e:
            #     return jsonify({'error': 'Bad Request', 'message': f'Request body must be in JSON format {e}'}), 400

        @self.app.route("/", methods=['GET'])
        def documentation():
            return send_from_directory('static', 'index.html')

        @self.app.errorhandler(405)
        def method_not_allowed(e):
            return jsonify({'error': 'Method Not Allowed', 'message': 'The method is not allowed for the requested URL.'}), 405

        @self.app.errorhandler(404)
        def page_not_found(e):
            return jsonify({'error': 'Invalid URL', 'detail': 'The requested URL is not found on the server.'}), 404

    def run(self, port=8000):
        self.app.run(host='0.0.0.0', port=port, debug=True)


ACCESS_KEY = config["aws"]['ACCESS_KEY']
BUCKET_NAME = config["aws"]['BUCKET_NAME']
OBJECT_KEY = config["aws"]['OBJECT_KEY']
SECRET_KEY = config["aws"]['SECRET_KEY']

algo = config["jwt_payload"]['algo']
iss = config["jwt_payload"]['iss']
aud = config["jwt_payload"]['aud']
exp_minutes = int(config["jwt_payload"]['exp_minutes'])
jti_prefix = config["jwt_payload"]['jti_prefix']
role = config["jwt_payload"]['role']
user = config["jwt_payload"]['user']
drl = config["jwt_payload"]['drl']

redshift_host = config['redshift']['host']
redshift_dbname = config['redshift']['dbname']
redshift_user = config['redshift']['user']
redshift_password = config['redshift']['password']
redshift_port = config['redshift']['port']

my_app = RestAPI(ACCESS_KEY, BUCKET_NAME, OBJECT_KEY, SECRET_KEY, algo, iss, aud, exp_minutes, jti_prefix, role, user, drl, redshift_host, redshift_dbname, redshift_user, redshift_password, redshift_port)
app=my_app.app
my_app.run()
