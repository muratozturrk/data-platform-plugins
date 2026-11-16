# Strimzi Kafka image + Ranger Kafka plugin (build args pin both)
ARG STRIMZI_IMAGE=quay.io/strimzi/kafka:0.46.0-kafka-3.9.1
FROM ${STRIMZI_IMAGE}
ARG RANGER_VERSION=2.8.0
USER root
# ranger-<ver>-kafka-plugin.tar.gz is built from the Ranger release and stored in the internal artifact repo
ADD https://artifacts.hkm.internal/ranger/ranger-${RANGER_VERSION}-kafka-plugin.tar.gz /tmp/plugin.tgz
RUN mkdir -p /opt/ranger-kafka-plugin && tar -xzf /tmp/plugin.tgz -C /opt/ranger-kafka-plugin --strip-components=1 \
 && cp -r /opt/ranger-kafka-plugin/lib/* /opt/kafka/libs/ \
 && rm /tmp/plugin.tgz
# audit-destination modules are separate since 2.7: add the ones in use (Elasticsearch, Kafka)
COPY audit-dest-libs/*.jar /opt/kafka/libs/ranger-kafka-plugin-impl/
USER 1001
