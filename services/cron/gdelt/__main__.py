import argparse
import asyncio
import logging
import os

from .config import GDELTConfig, KafkaConfig, LoggerConfig, SparkConfig
from .gdelt import GDELT
from .utils import now


def run(
  date: str,
  dest_path: str,
  broker: str,
  topic: str,
  semaphore: asyncio.Semaphore,
  logger_config: int,
):
  kc = KafkaConfig(broker, topic)
  gc = GDELTConfig(date, semaphore=semaphore, local_dest=dest_path)
  lc = LoggerConfig(logger_config)
  # TODO: master not local
  sc = SparkConfig(
    {
      'app_name': ('spark.app.name', 'GDELT_SPARK'),
      'master': ('spark.master', 'local'),
      'memory': ('spark.executor.memory', '4g'),
      'enable_log': ('spark.eventLog.enabled', 'true'),
      'parquet_dir': 'gdelt_ingestion_pruned_parquet',
    }
  )

  gdelt = GDELT(kafka_config=kc, gdelt_config=gc, logger_config=lc, spark_config=sc)

  # 3 tables, atomic
  try:
    asyncio.run(gdelt.ingest())  # Returns a list of file names in tests
  except Exception as e:
    print(e)
    # TODO: Insert an alert + backfill if any

  # Guaranteed there are three files
  asyncio.run(gdelt.run_once())


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Dyrmgraph CLI')
  parser.add_argument('--date', default=now(), help='Target date for the GDELT file')
  args = parser.parse_args()

  date = args.date
  GDELT_PATH = os.getenv('GDELT_PATH', '/data/gdelt')
  BROKER = os.getenv('BROKER', 'kafka:9092')
  RAW_TOPIC = os.getenv('INGEST_TOPIC', 'gdelt.raw')
  semaphore = asyncio.Semaphore(4)

  run(date, GDELT_PATH, BROKER, RAW_TOPIC, semaphore, logging.DEBUG)
