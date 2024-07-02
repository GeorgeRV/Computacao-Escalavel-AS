# Use the official Python base image
FROM python:3.9-buster

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install OpenJDK 11 and dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jre wget libpq-dev && \
    apt-get clean

# Get PostgreSQL driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2 /usr/local/spark && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

# Download GraphFrames JAR manually
RUN wget https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.1-s_2.12/graphframes-0.8.2-spark3.1-s_2.12.jar -P /usr/local/spark/jars

# Set environment variables for Spark
ENV SPARK_HOME /usr/local/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Set Python path for PySpark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.3-src.zip

# Copy application files
COPY . .

# Copy log4j properties
COPY log4j.properties /usr/local/spark/conf/

ENV PYTHONPATH "${PYTHONPATH}:/app"

# Run the application
CMD ["python", "/app/teste.py"]
