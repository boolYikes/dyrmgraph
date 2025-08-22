import asyncio
import logging

import pytest

import services.cron.gdelt.config as config
import services.cron.gdelt.gdelt as gd


@pytest.fixture
def gdelt_init_s1():
  return {
    'date': '20150218231530',
    'gdelt_path': '/lab/dee/repos_side/dyrmgraph/data/gdelt',
    'broker': 'localhost:9092',
    'topic': 'test.raw',
    'semaphore': asyncio.Semaphore(2),
    'logger': logging.DEBUG,
  }


# singleton
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
