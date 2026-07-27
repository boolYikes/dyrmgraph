FROM apache/spark:4.1.3-java17

USER root

RUN mkdir -p /opt/transform

COPY services/transform/target/transform.jar /opt/transform/transform.jar

RUN chown -R spark:spark /opt/transform

USER spark

WORKDIR /opt/transform