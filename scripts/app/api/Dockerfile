# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed dependencies specified in requirements.txt
RUN pip install -r requirements.txt

# Expose the port that your Flask app will run on (default is 5000)
EXPOSE 5000

# Define environment variables (optional)
ENV FLASK_APP=app.py
ENV FLASK_ENV=development

# Run the Flask app when the container launches
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
