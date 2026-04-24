FROM flink:1.20-scala_2.12-java11

# Python (needed by PyFlink; flink:1.20 is Debian bookworm → Python 3.10)
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink — must match the Flink image patch version (1.20.3)
RUN pip3 install --no-cache-dir apache-flink==1.20.3

# Kafka SQL connector (uber-jar, bundles kafka-clients; 3.3.0-1.20 covers all 1.20.x)
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# Avro + Confluent Schema Registry format support.
#
# Dependency chain (none of these are bundled in the base Flink image):
#   flink-avro-confluent-registry  → format JAR, shades SR client internally
#   flink-avro                     → Flink's Avro type utilities
#   avro                           → Apache Avro core (needs Jackson at runtime)
#   jackson-{core,databind,annotations} → Avro's JSON schema parsing
#
# Flink shades its own Jackson under a different package, so these don't conflict.
RUN wget -q -O /opt/flink/lib/flink-avro-confluent-registry.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/1.20.3/flink-avro-confluent-registry-1.20.3.jar && \
    wget -q -O /opt/flink/lib/flink-avro.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.20.3/flink-avro-1.20.3.jar && \
    wget -q -O /opt/flink/lib/avro.jar \
        https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar && \
    wget -q -O /opt/flink/lib/jackson-core.jar \
        https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar && \
    wget -q -O /opt/flink/lib/jackson-databind.jar \
        https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar && \
    wget -q -O /opt/flink/lib/jackson-annotations.jar \
        https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar

# Confluent Schema Registry client — flink-avro-confluent-registry is a thin JAR;
# the SR client is declared 'provided' so we must supply it ourselves.
# CachedSchemaRegistryClient uses Guava's LoadingCache, so guava must be on classpath too
# (Flink bundles guava only under its own shaded prefix, not as com.google.guava).
RUN wget -q -O /opt/flink/lib/kafka-schema-registry-client.jar \
        https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.6.0/kafka-schema-registry-client-7.6.0.jar && \
    wget -q -O /opt/flink/lib/guava.jar \
        https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar && \
    wget -q -O /opt/flink/lib/kafka-clients.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

RUN mkdir -p /opt/jobs
# Jobs are bind-mounted from ./flink-jobs at runtime (see docker-compose.yml).
# For a prod image, swap the bind-mount for: COPY --chown=flink:flink flink-jobs/ /opt/jobs/
