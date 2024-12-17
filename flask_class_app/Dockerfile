# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /ndx_rest_api_sender

# Copy the current directory contents into the container at /ndx_rest_api_sender
COPY . /ndx_rest_api_sender

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Run the application with Gunicorn
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "ndx_rest_api_sender:app"]
