import asyncio
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Literal, TypedDict

import pytest
from filelock import BaseFileLock, FileLock
from kafka.admin import KafkaAdminClient, NewTopic
from testcontainers.kafka import KafkaContainer

import services.cron.gdelt.config as config
import services.cron.gdelt.gdelt as gd

# TODO: No hardcoding... correct date to enum


class InitConfig(TypedDict):
  date: str
  gdelt_path: str
  broker: str
  topic: str
  semaphore: int
  logger: (
    Literal[0] | Literal[10] | Literal[20] | Literal[30] | Literal[40] | Literal[50]
  )
  outfile: str
  outfile_path: str


@pytest.fixture(scope='session')
def gdelt_init_s1() -> InitConfig:
  init_config: InitConfig = {
    'date': '20150218231530',
    'gdelt_path': '/lab/dee/repos_side/dyrmgraph/data/data_test',
    'broker': 'localhost:9093',
    'topic': 'test.raw',
    'semaphore': 2,
    'logger': logging.DEBUG,
    'outfile': f'{datetime.now().strftime("%Y-%d-%m %H:%M:%S")}.test_log.log',
    'outfile_path': '/lab/dee/repos_side/dyrmgraph/services/cron/tests/logs',
  }
  return init_config


@pytest.fixture(scope='session')
def init_lock(gdelt_init_s1) -> BaseFileLock:
  lock = FileLock(f'{gdelt_init_s1["gdelt_path"]}/.gdelt.lock')
  return lock


@pytest.fixture(scope='session')
def gdelt_init_s2(gdelt_init_s1: InitConfig) -> Callable[[], gd.GDELT]:
  def _init_factory():  # for semaphore
    kc = config.KafkaConfig(gdelt_init_s1['broker'], gdelt_init_s1['topic'])
    gc = config.GDELTConfig(
      gdelt_init_s1['date'],
      semaphore=asyncio.Semaphore(gdelt_init_s1['semaphore']),
      local_dest=gdelt_init_s1['gdelt_path'],
    )
    lc = config.LoggerConfig(
      level=gdelt_init_s1['logger'],
      log_file_handler=gdelt_init_s1['outfile'],
      log_file_out_path=gdelt_init_s1['outfile_path'],
    )
    return gd.GDELT(kafka_config=kc, gdelt_config=gc, logger_config=lc)

  return _init_factory


@pytest.fixture(
  scope='function',
)
def data_init_cleanup(gdelt_init_s1, init_lock):
  from services.cron.tests.helper import cleanup_data, init_data

  init_lock.acquire()
  init_data(gdelt_init_s1['gdelt_path'], '20150218231500')

  yield

  cleanup_data(gdelt_init_s1['gdelt_path'])
  init_lock.release()


@pytest.fixture(scope='session')
def kafka_bootstrap():
  # KRaft mode (no ZooKeeper). Testcontainers handles wiring.
  with KafkaContainer(
    image='confluentinc/cp-kafka:7.4.10', port=9093
  ).with_kraft() as kafka:
    bootstrap = kafka.get_bootstrap_server()
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


@pytest.fixture(scope='function')
def init_pub_sub(topic, gdelt_init_s2, kafka_bootstrap):
  import json
  import uuid

  from kafka import KafkaConsumer, KafkaProducer

  gd = gdelt_init_s2()

  val_ser = gd.kafka_config.value_serializer
  key_ser = gd.kafka_config.key_serializer
  key_deser = getattr(gd.kafka_config, 'key_deserializer', None)

  producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap,
    value_serializer=val_ser,
    key_serializer=key_ser,
    enable_idempotence=getattr(gd.kafka_config, 'enable_idempotence', False),
    acks=getattr(gd.kafka_config, 'acks', 'all'),
    compression_type=getattr(gd.kafka_config, 'compression_type', None),
    linger_ms=getattr(gd.kafka_config, 'linger_ms', 0),
    retries=getattr(gd.kafka_config, 'retries', 3),
    max_in_flight_requests_per_connection=getattr(
      gd.kafka_config, 'max_in_flight', 1
    ),  # if not 1, idempotence is not guaranteed
  )

  group_id = f'test_pubsub_{uuid.uuid4()}'  # fresh group each run
  consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_bootstrap,
    value_deserializer=lambda m: json.loads(m.decode()),
    key_deserializer=key_deser,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=group_id,
    consumer_timeout_ms=2000,  # don’t hang forever in tests
    # isolation_level='read_committed',  # for transactions
  )

  try:
    yield producer, consumer
  finally:
    try:
      consumer.close()
    finally:
      producer.flush()
      producer.close()
