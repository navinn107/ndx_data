# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /ndx_rest_api_sender

# Copy the current directory contents into the container at /app
COPY . /ndx_rest_api_sender

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


# Set the FLASK_APP environment variable to your Flask application module
ENV FLASK_APP=ndx_rest_api_sender:app

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Run the Flask app using the flask command
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]

