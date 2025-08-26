import asyncio
import logging

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from testcontainers.kafka import KafkaContainer

import services.cron.gdelt.config as config
import services.cron.gdelt.gdelt as gd

# TODO: No hardcoding


@pytest.fixture(scope='session')
def gdelt_init_s1():
  return {
    'date': '20150218231530',
    'gdelt_path': '/lab/dee/repos_side/dyrmgraph/data/gdelt',
    'broker': 'localhost:9093',
    'topic': 'test.raw',
    'semaphore': asyncio.Semaphore(2),
    'logger': logging.DEBUG,
  }


@pytest.fixture(scope='session')
def gdelt_init_s2(gdelt_init_s1):
  kc = config.KafkaConfig(gdelt_init_s1['broker'], gdelt_init_s1['topic'])
  gc = config.GDELTConfig(
    gdelt_init_s1['date'],
    semaphore=gdelt_init_s1['semaphore'],
    local_dest=gdelt_init_s1['gdelt_path'],
  )
  lc = config.LoggerConfig(gdelt_init_s1['logger'])
  return gd.GDELT(kafka_config=kc, gdelt_config=gc, logger_config=lc)


@pytest.fixture(scope='session')
def kafka_bootstrap():
  # KRaft mode (no ZooKeeper). Testcontainers handles wiring.
  with KafkaContainer(
    image='confluentinc/cp-kafka:7.4.10', port=9093
  ).with_kraft() as kafka:
    bootstrap = kafka.get_bootstrap_server()  # e.g. "localhost:9093"
    yield bootstrap


@pytest.fixture(scope='session')
def kafka_admin(kafka_bootstrap):
  admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap, client_id='test-admin')
  try:
    yield admin
  finally:
    admin.close()


@pytest.fixture
def topic(kafka_admin):
  import time

  name = 'test.raw'
  # Single-broker settings (partitions=1, RF=1)
  kafka_admin.create_topics(
    [NewTopic(name=name, num_partitions=1, replication_factor=1)]
  )
  # tiny settle to avoid rare metadata races on fresh brokers
  time.sleep(0.2)
  try:
    yield name
  finally:
    # Best-effort cleanup; ignore failures if broker disallows deletes
    try:
      kafka_admin.delete_topics([name])
    except Exception:
      pass


# TODO: consumer, producer fixture for indiv tests
