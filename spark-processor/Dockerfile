FROM python:3.10-bullseye

# Install Java 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variable  
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY spark_consumer.py .

# Run the Spark processor
CMD ["python", "-u", "spark_consumer.py"]