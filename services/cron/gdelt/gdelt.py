# TODO: Lazy loading if applicable
import asyncio
import csv
import io
import json
import os
import shutil
import uuid
import zipfile
from contextlib import suppress

import aiofiles
from aiohttp import ClientError, ClientSession, ClientTimeout
from kafka import KafkaProducer

from .config import GDELTConfig, KafkaConfig
from .logger import LoggingMixin
from .transform import transform
from .utils import (
  add_to_done_list,
  clean_up_ingestion,
  get_columns,
  get_url,
  is_done_with_ingestion,
  list_ready_files,
)


class FileUnavailableError(Exception): ...


class RetryableDownloadError(Exception): ...


class GDELT(LoggingMixin):
  def __init__(
    self, kafka_config: KafkaConfig, gdelt_config: GDELTConfig, *args, **kwargs
  ):
    self.kafka_config = kafka_config
    self.gdelt_config = gdelt_config
    self.urls, self.filenames, self.target_date = get_url(gdelt_config.date)
    super().__init__(*args, **kwargs)

  # Pass the method as an arg to asyncio.run
  async def ingest(self) -> list[str]:
    """
    Batch-atomic ingest of GDELT files.
    - Writes to a staging dir first; commits only if ALL succeed.
    - Returns final file paths (same order as inputs) iff batch committed.
    - Raises on 404 or after exhausting retries; nothing is committed in that case.
    """
    if len(self.urls) != len(self.filenames):
      raise ValueError('urls and filenames must have the same length!')

    os.makedirs(self.gdelt_config.local_dest, exist_ok=True)
    staging_dir = os.path.join(
      self.gdelt_config.local_dest, f'.staging-{uuid.uuid4().hex}'
    )
    os.makedirs(staging_dir, exist_ok=True)
    timeout = ClientTimeout(total=30)

    async def _download_one(idx: int, url: str, fname: str, session: ClientSession):
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

  def extract(self) -> dict[str, list[dict[str, str]]]:
    """
    - Reads all GDELT tables for the given date
    - Unzips them, reads, merges three tables into one dictionary
    - Labels records with actual column names
    - Returns the resulting document
    - Cleans up the original file
    """
    # don't read if a write is in progress
    # they only have three tables per date, guaranteed
    files = list_ready_files(
      self.gdelt_config.date,
      self.gdelt_config.local_dest,
      self.gdelt_config.archive_suffix,
    )

    data = {'export': [], 'gkg': [], 'mentions': []}
    for f in files:
      table = f.split('.')[1]
      columns: dict[str, str] = get_columns(self.gdelt_config.local_dest, table)
      with zipfile.ZipFile(f) as zf:
        for name in zf.namelist():
          if name.endswith('.csv'):
            with zf.open(name, 'r') as raw:
              # decode streaming; GDELT is tab-delimited
              text = io.TextIOWrapper(raw, encoding='utf-8', errors='ignore')
              reader = csv.reader(
                text, delimiter='\t'
              )  # snapshot opener -> non-blocking

              # select columns
              for row in reader:
                labeled_row: dict = {}
                for i, record in enumerate(row):
                  if columns.get(str(i), None):
                    labeled_row[columns[str(i)]] = record
                data[table].append(labeled_row)
    return data

  # TODO: validate
  async def run_once(self):
    """
    1. Reads a *TRANSFORMED* GDELT data
    2. reads contents and then publishes to a Kafka topic, row by row
    """
    if is_done_with_ingestion(
      self.gdelt_config.date,
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

      data = self.extract()
      final = transform(data)

      # throughput: every 15 min, 9000-something-rows give or take == 10 rows/sec
      for row in final.iter_rows(named=True):
        # key = f"{row['GlobalEventID']}|{row['doc_url']}|{row['SentenceID']}"
        # gdelt-row is a fire-and-forget topic(cuz it's staging),
        # -hence we don't use key, random sticky partitioning
        prod.send(self.kafka_config.topic, value=json.dumps(row))
        prod.flush()

        # Mark as processed, cleanup
        add_to_done_list(self.gdelt_config.local_dest, self.gdelt_config.date)
        clean_up_ingestion(self.gdelt_config.date, self.gdelt_config.local_dest)

    else:
      # Three files are guaranteed
      # *BUT* if for some reason (manually deleted the sentinel or sometihng...)
      # it boils down to this block
      # TODO: handle it: backfill via kafka
      raise Exception(
        f'File(s) missing. Backfill signal sent for {self.gdelt_config.date}'
      )
