# TODO: Lazy loading if applicable
import asyncio
import os

from .config import GDELTConfig, KafkaConfig, SparkConfig
from .logger import LoggingMixin
from .utils import (
  add_to_done_list,
  clean_up_ingestion,
  get_url,
  is_done_with_ingestion,
  list_ready_files,
)


class FileUnavailableError(Exception): ...


class RetryableDownloadError(Exception): ...


class GDELT(LoggingMixin):
  def __init__(
    self,
    kafka_config: KafkaConfig,
    gdelt_config: GDELTConfig,
    spark_config: SparkConfig,
    *args,
    **kwargs,
  ):
    self._kafka_config: KafkaConfig = kafka_config
    self._gdelt_config: GDELTConfig = gdelt_config
    self._spark_config: SparkConfig = spark_config
    self._urls, self._filenames, self._target_date = get_url(gdelt_config.date)
    # settling types
    self._urls: list[str]
    self._filenames: list[str]
    self._target_date: str
    super().__init__(*args, **kwargs)

  @property
  def kafka_config(self):
    return self._kafka_config

  @property
  def gdelt_config(self):
    return self._gdelt_config

  @property
  def spark_config(self):
    return self._spark_config

  @property
  def urls(self):
    return self._urls

  @property
  def filenames(self):
    return self._filenames

  @property
  def target_date(self):
    return self._target_date

  # TODO: Validators - validate configs in their own classes.
  # Need to validate urls, filenames, target_date. regex?
  @kafka_config.setter
  def kafka_config(self, val):
    self._kafka_config = val

  @gdelt_config.setter
  def gdelt_config(self, val):
    self._gdelt_config = val

  @spark_config.setter
  def spark_config(self, val):
    self._spark_config = val

  @urls.setter
  def urls(self, val):
    self._urls = val

  @filenames.setter
  def filenames(self, val):
    self._filenames = val

  @target_date.setter
  def target_date(self, val):
    self._target_date = val

  # Pass the method as an arg to asyncio.run
  async def ingest(self) -> list[str]:
    """
    Batch-atomic ingest of GDELT files.
    - Writes to a staging dir first; commits only if ALL succeed.
    - Returns final file paths (same order as inputs) iff batch committed.
    - Raises on 404 or after exhausting retries; nothing is committed in that case.
    """
    import shutil
    import uuid
    from contextlib import suppress

    from aiohttp import ClientError, ClientSession, ClientTimeout

    if len(self.urls) != len(self.filenames):
      raise ValueError('urls and filenames must have the same length!')

    os.makedirs(self.gdelt_config.local_dest, exist_ok=True)
    staging_dir = os.path.join(
      self.gdelt_config.local_dest, f'.staging-{uuid.uuid4().hex}'
    )
    os.makedirs(staging_dir, exist_ok=True)
    timeout = ClientTimeout(total=30)

    async def _download_one(idx: int, url: str, fname: str, session: ClientSession):
      import aiofiles

      staging_path = os.path.join(staging_dir, fname.lower())
      attempt = 0
      while True:
        try:
          async with self.gdelt_config.semaphore:
            async with session.get(url, allow_redirects=True) as resp:
              if resp.status == 404:
                raise FileUnavailableError(f'File not found: {url}')
              if resp.status != 200:
                raise RetryableDownloadError(
                  f'Unexpected status {resp.status} for {url}'
                )
              async with aiofiles.open(staging_path, 'wb') as f:
                async for chunk in resp.content.iter_chunked(1 << 20):
                  if chunk:
                    await f.write(chunk)
              return (
                idx,
                staging_path,
                os.path.join(self.gdelt_config.local_dest, fname.lower()),
              )
        except (TimeoutError, ClientError, RetryableDownloadError) as e:
          attempt += 1
          if attempt >= self.gdelt_config.retries:
            raise RetryableDownloadError(
              f'DWN failure: {url} Attempts: {self.gdelt_config.retries} MSG: {e}'
            ) from e
          await asyncio.sleep(self.gdelt_config.backoff * (2 ** (attempt - 1)))

    async with ClientSession(timeout=timeout) as session:
      tasks = [
        asyncio.create_task(_download_one(i, u, f, session))
        for i, (u, f) in enumerate(zip(self.urls, self.filenames, strict=False))
      ]
      try:
        triples = await asyncio.gather(*tasks)  # raises if any task failed
      except Exception:
        for t in tasks:
          t.cancel()
        with suppress(Exception):
          await asyncio.gather(*tasks, return_exceptions=True)
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    # Commit phase: move all files into place, then write sentinel
    results = [''] * len(self.filenames)
    try:
      for i, staging_path, final_path in sorted(triples, key=lambda x: x[0]):
        os.replace(staging_path, final_path)  # atomic per file on same filesystem
        results[i] = final_path
      # Sentinel indicates the whole batch is present
      with open(
        os.path.join(
          self.gdelt_config.local_dest,
          f'{self.target_date}{self.gdelt_config.sentinel_name}',
        ),
        'w',
      ) as sf:
        sf.write('ok\n')
    finally:
      shutil.rmtree(staging_dir, ignore_errors=True)

    return results

  def extract(self):
    """
    - Reads all GDELT tables for the given date
    - Unzips them, saves back pruned csvs as parquet
    """
    import zipfile
    from pathlib import Path

    from .transform import prune_columns

    # don't read if a write is in progress
    # they only have three tables per date, guaranteed
    files = list_ready_files(
      self.target_date,
      self.gdelt_config.local_dest,
      self.gdelt_config.archive_suffix,
    )

    # where the requested column list and column dictionary are
    data_path = str(Path(self.gdelt_config.local_dest).parent)

    for f in files:
      p = Path(f)
      table = f.split('.')[1]
      with zipfile.ZipFile(p) as zf:
        # probably contains only one. never seen one more than one
        for name in zf.namelist():
          if name.lower().endswith('.csv'):
            zf.extractall(p.parent)
            prune_columns(
              self.target_date,
              data_path,
              data_path,
              dict(self.spark_config),
              os.path.join(self.gdelt_config.local_dest, name),
              table,
              self.gdelt_config.local_dest,
              self.spark_config['parquet_dir'],
              logger=self.logger,
            )

  # TODO: validate
  async def run_once(self):
    """
    1. Reads ingested GDELT tables
    2. Extracts csv.zips, selects, prune and write to parquet
    3. reads, transforms and then publishes to a Kafka topic, row by row
    """
    import json

    from kafka import KafkaProducer

    from .transform import transform

    if is_done_with_ingestion(
      self.target_date,
      self.gdelt_config.sentinel_name,
      self.gdelt_config.local_dest,
    ):
      prod = KafkaProducer(
        bootstrap_servers=self.kafka_config.bootstrap_servers,
        value_serializer=self.kafka_config.value_serializer,
        key_serializer=self.kafka_config.key_serializer,
        enable_idempotence=self.kafka_config.enable_idempotence,  # dedupe on broker
        acks=self.kafka_config.acks,  # durability
        compression_type=self.kafka_config.compression_type,
        linger_ms=self.kafka_config.linger_ms,
      )

      self.extract()  # -> 3 parquets done, saved
      parquet_path = os.path.join(
        self.gdelt_config.local_dest, self.spark_config['parquet_dir']
      )
      final = transform(
        self.target_date, dict(self.spark_config), parquet_path
      )  # -> read 3 parquets

      # throughput: every 15 min, 9000-something-rows give or take == 10 rows/sec
      for row in final.collect():
        # probably no dupe but ... GDELT maintainers are humans... right? 😯
        prod.send(
          self.kafka_config.topic, value=json.dumps(row), key=row['message_key']
        )
        prod.flush()

        # Mark as processed, cleanup
        add_to_done_list(self.gdelt_config.local_dest, self.target_date)
        clean_up_ingestion(self.target_date, self.gdelt_config.local_dest)

    else:
      # Three files are guaranteed
      # *BUT* if for some reason (manually deleted the sentinel or sometihng...)
      # it boils down to this block
      # TODO: handle it: backfill via kafka
      raise FileUnavailableError(
        f'File(s) missing. Backfill signal sent for {self.target_date}'
      )
