# stage 1
FROM alpine:3.21 AS jar-downloader

RUN apk add --no-cache curl=8.14.1-r2 ca-certificates=20260909-r0

ARG POSTGRES_VERSION=42.7.7
ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG S3_ACCELERATOR_VERSION=1.2.1

WORKDIR /jars

RUN set -eux; \
    BASE=https://repo.maven.apache.org/maven2; \
    curl -fSL --retry 3 \
      "$BASE/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" \
      -o postgresql-${POSTGRES_VERSION}.jar; \
    curl -fSL --retry 3 \
      "$BASE/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      -o hadoop-aws-${HADOOP_VERSION}.jar; \
    curl -fSL --retry 3 \
      "$BASE/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
      -o bundle-${AWS_SDK_VERSION}.jar; \
    curl -fSL --retry 3 \
      "$BASE/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${S3_ACCELERATOR_VERSION}.jar" \
      -o analyticsaccelerator-s3-${S3_ACCELERATOR_VERSION}.jar

# stage 2
FROM apache/spark:4.1.3-java21-scala

USER root

RUN mkdir -p /opt/transform /opt/spark/work-dir

# deps to the spark class path
COPY --from=jar-downloader /jars/ /opt/spark/jars/

COPY services/transform/target/transform.jar /opt/transform/transform.jar

RUN chown -R spark:spark /opt/transform /opt/spark/work-dir

USER spark

WORKDIR /opt/spark/work-dir