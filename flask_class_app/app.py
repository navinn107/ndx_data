from flask import Flask, jsonify

class MyFlaskApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        @self.app.route('/')
        def home():
            return jsonify({"message": "Welcome to the Flask Class-Based App!"})

        @self.app.route('/api/data')
        def get_data():
            return jsonify({"data": [1, 2, 3, 4, 5]})

    def run(self):
        self.app.run(host='0.0.0.0', port=5000, debug=True)

if __name__ == '__main__':
    my_app = MyFlaskApp()
    my_app.run()
