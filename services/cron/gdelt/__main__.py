import argparse
import asyncio
import logging
import os

# TODO: conditional imports depending on env (test/prod)
from services.cron.gdelt.config import GDELTConfig, KafkaConfig, LoggerConfig
from services.cron.gdelt.gdelt import GDELT
from services.cron.gdelt.utils import now

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Dyrmgraph CLI')
  parser.add_argument('--date', default=now(), help='Target date for the GDELT file')
  args = parser.parse_args()

  date = args.date
  GDELT_PATH = os.getenv('GDELT_PATH', '/data/gdelt')
  BROKER = os.getenv('BROKER', 'kafka:9092')
  RAW_TOPIC = os.getenv('INGEST_TOPIC', 'gdelt.raw')
  semaphore = asyncio.Semaphore(4)

  kc = KafkaConfig(BROKER, RAW_TOPIC)
  gc = GDELTConfig(date, semaphore=semaphore, local_dest=GDELT_PATH)
  lc = LoggerConfig(logging.DEBUG)

  gdelt = GDELT(kafka_config=kc, gdelt_config=gc, logger_config=lc)

  # ingest and transform runs sequentially, run_once runs separately.
  # both groups run periodically
  # Three table ingestion, atomic
  try:
    results = asyncio.run(gdelt.ingest())
  except Exception as e:
    print(e)
    # TODO: Insert an alert + backfill if any

  # Guaranteed there are three files
  gdelt.run_once()
