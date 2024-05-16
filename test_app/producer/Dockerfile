# Start with a Debian stable image
FROM debian:stable

# Update and install utilities, Java, Python, and pip
RUN apt-get update -qq && apt-get install -y wget curl tar python3 python3-venv python3-pip

# Download and extract OpenJDK
RUN wget https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz -P /tmp &&\
    mkdir /usr/java &&\
    tar -xzf /tmp/openjdk-11.0.1_linux-x64_bin.tar.gz --strip-components=1 -C /usr/java

# Set Java environment variables
ENV JAVA_HOME /usr/java
ENV PATH $PATH:$JAVA_HOME/bin

# Create a virtual environment and activate it
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Kafka
ENV KAFKA_HOME=/opt/kafka
ENV KAFKA_VERSION=2.4.0
ENV SCALA_VERSION=2.12

RUN curl -O https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && mkdir $KAFKA_HOME \
    && tar -xvzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C $KAFKA_HOME --strip-components=1 \
    && rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Set Kafka environment variables
ENV PATH $PATH:$KAFKA_HOME/bin

# Install Python packages within the virtual environment
RUN pip3 install confluent-kafka requests Flask

# Copy the Kafka producer script
COPY producer.py /app/producer.py

# Set the default command
CMD ["python3", "-u", "/app/producer.py"]

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Define working directory
WORKDIR /app