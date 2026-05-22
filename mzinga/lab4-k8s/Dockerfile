# Specify the base image used to run the application
FROM python:3.12-slim

# Set the working directory (where the application code will be copied and executed)
WORKDIR /app

# Copy the application code (app.py) from the local directory to the working directory in the container
COPY app.py .

# Define build-time arguments for application version and color, with default values
ARG APP_VERSION=1.0.0
ARG APP_COLOR=blue

# Set environment variables for the application version and color, which can be accessed by the application code
ENV APP_VERSION=${APP_VERSION}
ENV APP_COLOR=${APP_COLOR}
ENV PORT=8080

# Expose the port that the application will listen on (8080 in this case)
EXPOSE 8080

# Define the command to run the application when the container starts. This will execute the app.py script using Python.
CMD ["python", "app.py"]