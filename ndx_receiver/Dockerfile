# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /ndx_receiver

# Copy the current directory contents into the container at /app
COPY . /ndx_receiver

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run app.py when the container launches
CMD ["python3", "ndx_receiver.py"]
