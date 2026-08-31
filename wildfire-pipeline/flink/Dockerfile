FROM flink:1.18.1-scala_2.12-java11

# Let pip install into the system env even on PEP 668 bases (harmless on older pip)
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# Python + tools
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink must match the Flink runtime version; influxdb-client for the sinks
RUN pip3 install --no-cache-dir apache-flink==1.18.1 influxdb-client==1.44.0

# Kafka connector JAR (verified for Flink 1.18) into Flink's lib dir
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar

# The PyFlink job (present in jobmanager, taskmanager, and submitter)
COPY wildfire_job.py /opt/flink/usrlib/wildfire_job.py
