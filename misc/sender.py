from flask import Flask, request, jsonify, send_from_directory
from flasgger import Swagger
import pika
import uuid
import logging as log
import time
import json
import ssl
import configparser


config = configparser.ConfigParser()
config.read('rabbitmq_config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPI:

    def __init__(self, rabbitmq_user, rabbitmq_password, rabbitmq_broker_id, region, rabbitmq_port, cipher_text, exchange_name, exchange_type_name, routing_key_name, queue_name, timeout):
        
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_password = rabbitmq_password
        self.rabbitmq_broker_id = rabbitmq_broker_id
        self.region = region
        self.rabbitmq_port = rabbitmq_port
        self.cipher_text = cipher_text

        self.exchange = exchange_name
        self.exchange_type = exchange_type_name
        self.routing_key = routing_key_name
        self.queue = queue_name
        self.timeout = timeout  
                
        self.connection = None
        self.channel = None
        self.reply_queue = None
        
        self.connect()

        self.app = Flask(__name__)
        self.swagger = Swagger(self.app)  # Initialize Swagger with Flask app
        self.setup_routes()

    def connect(self):

        """ESTABLISHES CONNECTION TO RABBITMQ AND SETS UP CHANNEL, EXCHANGE, AND QUEUE."""
        
        self.response_json = None
        self.correlation_id = None

        # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers(self.cipher_text)

        url = f"amqps://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_broker_id}.mq.{self.region}.amazonaws.com:{self.rabbitmq_port}"
        parameters = pika.URLParameters(url)
        parameters.ssl_options = pika.SSLOptions(context=ssl_context)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
                
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)
        
        self.reply_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        
        log.info(".......SERVER INITIALIZATION DONE.......")
        
        self.channel.basic_consume(queue=self.reply_queue, on_message_callback=self.on_reply_message_received, auto_ack=True)

    def on_reply_message_received(self, ch, method, properties, body):
        
        if self.correlation_id == properties.correlation_id:
            self.response_json = json.loads(body)

    def basic_publish(self, message):
        
        self.channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key,
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
            if self.connection.is_closed or not self.channel or self.channel.is_closed:
                log.info(".......CONNECTION OR CHANNEL IS CLOSED, RECONNECTING.......")
                self.connect()

            self.basic_publish(message)
        
        except (pika.exceptions.StreamLostError, pika.exceptions.AMQPHeartbeatTimeout):
            log.info(".......STREAM LOST THE CONNECTION OR CHANNEL IS CLOSED, RECONNECTING.......")
            self.connect()
            self.basic_publish(message)

        try:
            self.connection.process_data_events(time_limit=self.timeout)
        
        except pika.exceptions.AMQPTimeoutError:
            return None
        
        return self.response_json
    
    def setup_routes(self):
        
        """SETS UP THE FLASK ROUTE FOR THE API."""
        
        @self.app.route('/api/ndx/get-data', methods=['GET'])
        def get_info():
            """
            Endpoint to fetch data based on MSISDN value.
            ---
            parameters:
              - name: msisdn
                in: query
                type: string
                required: true
                description: The MSISDN value
            responses:
              200:
                description: Successful response with data
              400:
                description: Missing MSISDN value
              500:
                description: No response from server
            """
            
            msisdn_val = request.args.get('msisdn')            
            if not msisdn_val:
                return jsonify({"statusCode": 400, "detail": "MSISDN VALUE IS MISSING"}), 400
            
            message = json.dumps({'msisdn': msisdn_val})
            
            try:
                        
                response = self.publish(message)
                if response is None:
                    return jsonify({"statusCode": 500, "detail": "INTERNAL SERVER ERROR"}), 500
                
                return jsonify(response), response["statusCode"]

            except Exception as e:
                return jsonify({"statusCode": 500, "detail": "INTERNAL SERVER ERROR - " + str(e)}), 500
        
        @self.app.route('/', methods=['GET'])   
        def api_docs():
            return send_from_directory('static', 'index.html')

        def method_not_allowed(e):
            return jsonify({'error': 'Method Not Allowed', 'detail': 'The method is not allowed for the requested URL.'}), 405

        @self.app.errorhandler(404)
        def page_not_found(e):
            return jsonify({'error': 'Invalid URL', 'detail': 'The requested URL is not found on the server. It is to fetch ndx africa data'}), 404

            
        

    def run(self, port=7000):
        
        """RUNS THE FLASK APPLICATION."""
        self.app.run(host='0.0.0.0', port=port, debug=True)

# # Accessing the values
timeout = int(config['default']['timeout'])
rabbitmq_user = config['rabbitmq']['user']
rabbitmq_password = config['rabbitmq']['password']
rabbitmq_broker_id = config['rabbitmq']['broker_id']
rabbitmq_port = int(config['rabbitmq']['port'])
rabbitmq_region = config['rabbitmq']['region']
cipher_text = config['rabbitmq']['cipher_text']
exchange_name = config['exchange']['name']
exchange_type_name = config['exchange']['type']
routing_key_name = config['routing']['key']
queue_name = config['queue']['name']


my_app = RestAPI( rabbitmq_user, rabbitmq_password, rabbitmq_broker_id, rabbitmq_region, rabbitmq_port, cipher_text, exchange_name, exchange_type_name, routing_key_name, queue_name, timeout)
#my_app.run()
app = my_app.app
