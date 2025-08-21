import glob
import json
import os
from datetime import datetime, timedelta


def now() -> str:
  """Returns the date now in YYYYMMDDHHMMSS format"""
  return datetime.now().strftime('%Y%m%d%H%M%S')


def closest_15min_before(ts_str: str) -> str:
  """
  Accepts a timestamp in YYYYMMDDHHMMSS string format,
  converts it to the closest time frame before the input
  """
  dt = datetime.strptime(ts_str, '%Y%m%d%H%M%S')
  minutes_past = dt.minute % 15
  dt_floor = dt - timedelta(
    minutes=minutes_past, seconds=dt.second, microseconds=dt.microsecond
  )

  return dt_floor.strftime('%Y%m%d%H%M%S')


def get_url(date: str) -> tuple[list[str], list[str]]:
  """
  Builds urls and filenames based on input date and returns it
  """
  BASE_URL = 'http://data.gdeltproject.org/gdeltv2/'
  TABLES = ['gkg', 'mentions', 'export']
  target_date = closest_15min_before(date)
  filenames = [
    f'{target_date}.{table}{".csv.zip" if table == "gkg" else ".CSV.zip"}'
    for table in TABLES
  ]
  urls = [os.path.join(BASE_URL, filename) for filename in filenames]
  return urls, filenames, target_date


def list_ready_files(date, dest_dir='/data/gdelt', suffix='.csv.zip') -> list[str]:
  files = glob.glob(os.path.join(dest_dir, f'{glob.escape(date)}*{suffix}'))
  return [f for f in files if f.endswith(suffix) and not f.endswith(suffix + '.part')]


def add_to_done_list(local_dest, date):
  """
  Marks a date as completed (transform)
  """
  manifest_dest = os.path.join(local_dest, 'done_list.txt')
  with open(manifest_dest, 'a') as f:
    f.write(f'{date}\n')


def get_columns(data_path, table) -> dict[str, str]:
  """
  - Returns a list of columns to help selecting specific columns
  - The content is in 'column_name: real_index format'
  """
  path = os.path.join(data_path, 'target_columns2.json')
  with open(path) as f:
    data = json.load(f)
    # map column names and indexes for transform
  return data[table]


def is_done_with_ingestion(date: str, sentinel_name: str, dest_path='/data/gdelt'):
  return os.path.exists(os.path.join(dest_path, f'{date}{sentinel_name}'))


def clean_up_ingestion(date: str, dest_path='/data/gdelt'):
  files = glob.glob(os.path.join(dest_path, f'*{glob.escape(date)}*'))
  for f in files:
    os.remove(f)
