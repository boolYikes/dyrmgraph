import json
import logging
from asyncio import Semaphore
from collections.abc import Callable
from typing import TypedDict

# TODO: encapsulate, validate -> Or as TypedDicts(TBD)


class LoggerConfig:
  def __init__(
    self,
    level=logging.INFO,
    log_format='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
    date_format='%Y-%m-%d %H:%M:%S',
    log_file_handler='',
    log_file_out_path='',
  ):
    if bool(log_file_out_path) ^ bool(log_file_handler):
      raise Exception(
        'You must specify both log file handler and the output path or not at all'
      )
    self.level: int = level
    self.log_format: str = log_format
    self.date_format: str = date_format
    self.log_file_handler: str = log_file_handler
    self.log_file_out_path: str = log_file_out_path


class KafkaConfig:
  def __init__(
    self,
    bootstrap_servers: str,
    topic: str,
    value_serializer: Callable = lambda v: json.dumps(v).encode('utf-8'),
    key_serializer: Callable = lambda k: k.encode(),
    enable_idempotence: bool = True,
    acks: str = 'all',
    compression_type: str = 'zstd',
    linger_ms: int = 20,
  ):
    self.bootstrap_servers = bootstrap_servers
    self.value_serializer = value_serializer
    self.key_serializer = key_serializer
    self.enable_idempotence = enable_idempotence
    self.acks = acks
    self.compression_type = compression_type
    self.linger_ms = linger_ms
    self.topic = topic


# TODO: TBD
class SparkConfig(TypedDict):
  """
  - master: local or spark:// ...
  - memory: e.g. 4 means 4g
  - enable_log: bool in lower case
  - Example:
    ('spark.master', master),
    ('spark.executor.memory', memory),
    ('spark.app.name', app_name),
    ('spark.eventLog.enabled', enable_log),
  """

  master: tuple[str, str]
  app_name: tuple[str, str]
  enable_log: tuple[str, str]
  memory: tuple[str, str]
  parquet_dir: str


# This would include ingest AND ETL
class GDELTConfig:
  def __init__(
    self,
    date: str,
    semaphore: Semaphore,
    local_dest: str = 'data/gdelt',
    sentinel_name: str = '.gdelt_batch_complete',
    retries: int = 3,
    backoff: float = 0.5,
    archive_suffix: str = '.csv.zip',
  ):
    self.date = date
    self.local_dest = local_dest
    self.sentinel_name = sentinel_name
    self.semaphore = semaphore
    self.retries = retries
    self.backoff = backoff
    self.archive_suffix = archive_suffix


# Test code NOT USED
# if __name__ == '__main__':
#   kfk_config = KafkaConfig(bootstrap_servers='kafka:9090')
#   print(kfk_config.__dir__())
#   print(kfk_config.__getstate__())
