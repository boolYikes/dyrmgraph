import json
import logging
from asyncio import Semaphore


class LoggerConfig:
  def __init__(
    self,
    level=logging.INFO,
    log_format='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
    date_format='%Y-%m-%d %H:%M:%S',
  ):
    self.level = level
    self.log_format = log_format
    self.date_format = date_format


class KafkaConfig:
  def __init__(
    self,
    bootstrap_servers: str,
    topic: str,
    value_serializer: callable = lambda v: json.dumps(v).encode('utf-8'),
    key_serializer: callable = lambda k: k.encode(),
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


# Test code
if __name__ == '__main__':
  kfk_config = KafkaConfig(bootstrap_servers='kafka:9090')
  print(kfk_config.__dir__())
  print(kfk_config.__getstate__())
