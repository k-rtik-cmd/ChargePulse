# Build context: repository root (see docker-compose.yml)
FROM apache/spark:4.0.1

USER root
ENV TZ=UTC \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --default-timeout=120 --retries 10 -r /tmp/requirements.txt

# Bake the Kafka connector (and its exact transitive versions) into the image
# so containers do not download jars at start-up.
COPY spark/noop.py /tmp/noop.py
RUN /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 /tmp/noop.py \
 && cp /tmp/ivy/jars/org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.1.jar \
       /tmp/ivy/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
       /tmp/ivy/jars/org.apache.kafka_kafka-clients-*.jar \
       /tmp/ivy/jars/org.apache.commons_commons-pool2-*.jar \
       /opt/spark/jars/ \
 && rm -rf /tmp/ivy /tmp/noop.py /root/.ivy2*

RUN mkdir -p /opt/chargepulse /var/lib/chargepulse/checkpoints /var/lib/chargepulse/batch-work \
 && chown -R spark:spark /var/lib/chargepulse

COPY chargepulse_common /opt/chargepulse/chargepulse_common
COPY config /opt/chargepulse/config
COPY streaming /opt/chargepulse/streaming
COPY batch /opt/chargepulse/batch
COPY tests /opt/chargepulse/tests
COPY producer/simulator.py /opt/chargepulse/producer/simulator.py

ENV PYTHONPATH=/opt/chargepulse:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    TARIFF_PATH=/opt/chargepulse/config/tariffs.csv
USER spark
WORKDIR /opt/chargepulse
