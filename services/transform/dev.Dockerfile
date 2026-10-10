FROM apache/spark:4.1.3-java21-scala

USER root

RUN mkdir -p /opt/transform /opt/spark/work-dir

COPY services/transform/target/transform.jar /opt/transform/transform.jar

RUN chown -R spark:spark /opt/transform /opt/spark/work-dir

USER spark

WORKDIR /opt/spark/work-dir