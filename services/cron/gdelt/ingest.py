import asyncio
import os
import shutil
import uuid
from contextlib import suppress

import aiofiles
from aiohttp import ClientError, ClientSession, ClientTimeout


class FileUnavailableError(Exception): ...


class RetryableDownloadError(Exception): ...


async def ingest(
  target_date: str,
  urls: list[str],
  semaphore: asyncio.Semaphore,
  filenames: list[str],
  dest_dir: str = '/data/gdelt',
  retries: int = 3,
  backoff: float = 0.5,
  sentinel_name: str = '.gdelt_batch_complete',  # readers check this
) -> list[str]:
  """
  Batch-atomic ingest of GDELT files.
  - Writes to a staging dir first; commits only if ALL succeed.
  - Returns final file paths (same order as inputs) iff batch committed.
  - Raises on 404 or after exhausting retries; nothing is committed in that case.
  """
  if len(urls) != len(filenames):
    raise ValueError('urls and filenames must have the same length')

  os.makedirs(dest_dir, exist_ok=True)
  staging_dir = os.path.join(dest_dir, f'.staging-{uuid.uuid4().hex}')
  os.makedirs(staging_dir, exist_ok=True)
  timeout = ClientTimeout(total=30)

  async def _download_one(idx: int, url: str, fname: str, session: ClientSession):
    staging_path = os.path.join(staging_dir, fname.lower())
    attempt = 0
    while True:
      try:
        async with semaphore:
          async with session.get(url, allow_redirects=True) as resp:
            if resp.status == 404:
              raise FileUnavailableError(f'File not found: {url}')
            if resp.status != 200:
              raise RetryableDownloadError(f'Unexpected status {resp.status} for {url}')
            async with aiofiles.open(staging_path, 'wb') as f:
              async for chunk in resp.content.iter_chunked(1 << 20):
                if chunk:
                  await f.write(chunk)
            return idx, staging_path, os.path.join(dest_dir, fname.lower())
      except (TimeoutError, ClientError, RetryableDownloadError) as e:
        attempt += 1
        if attempt >= retries:
          raise RetryableDownloadError(
            f'Download failed for {url} after {retries} attempts: {e}'
          ) from e
        await asyncio.sleep(backoff * (2 ** (attempt - 1)))

  async with ClientSession(timeout=timeout) as session:
    tasks = [
      asyncio.create_task(_download_one(i, u, f, session))
      for i, (u, f) in enumerate(zip(urls, filenames, strict=False))
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
  results = [''] * len(filenames)
  try:
    for i, staging_path, final_path in sorted(triples, key=lambda x: x[0]):
      os.replace(staging_path, final_path)  # atomic per file on same filesystem
      results[i] = final_path
    # Sentinel indicates the whole batch is present
    with open(os.path.join(dest_dir, f'{target_date}{sentinel_name}'), 'w') as sf:
      sf.write('ok\n')
  finally:
    shutil.rmtree(staging_dir, ignore_errors=True)

  return results
