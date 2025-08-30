import glob
import json
import logging
import os
import sys
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
  logging.Formatter(
    fmt='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
  )
)
logger.addHandler(handler)
logger.propagate = False


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


def get_url(date: str) -> tuple[list[str], list[str], str]:
  """
  Builds urls and filenames based on input date and returns it
  """
  BASE_URL = 'http://data.gdeltproject.org/gdeltv2/'
  TABLES = ['gkg', 'mentions', 'export']
  target_date = closest_15min_before(date)
  filenames = [
    f'{target_date}.{table}{".csv.zip" if table == "gkg" else ".CSV.zip"}'
    for table in TABLES
  ]  # only the url is in uppercase. filenames are in lowercase
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


def extract_requested_columns(
  dic_path: str,
  result_path: str,
  request_path: str,
  log_level=logging.INFO,
  dic_name='column_dictionary.json',
  result_name='target_columns2.json',
  request_name='request_columns.json',
):
  """
  Takes a user-input column names to extract column indexes as a JSON file
  """
  try:
    with open(os.path.join(request_path, request_name)) as f:
      req: dict[str, list[str]] = json.load(f)
    with open(os.path.join(dic_path, dic_name)) as f:
      dic: dict[str, dict[str, str]] = json.load(f)
    result_dict = {}
    for table, req_cols in req.items():
      result_dict[table] = {
        idx: col_name
        for idx, col_name in dic[table].items()
        if col_name in req_cols  # Not much columns so no set()
      }
    with open(os.path.join(result_path, result_name), 'w') as f:
      json.dump(result_dict, f)

  except BaseException as e:
    logger.log(msg=e, level=log_level)
    raise BaseException from e


def is_done_with_ingestion(date: str, sentinel_name: str, dest_path='/data/gdelt'):
  return os.path.exists(os.path.join(dest_path, f'{date}{sentinel_name}'))


def clean_up_ingestion(date: str, dest_path='/data/gdelt'):
  files = glob.glob(os.path.join(dest_path, f'*{glob.escape(date)}*'))
  for f in files:
    os.remove(f)


if __name__ == '__main__':
  data_path = '/lab/dee/repos_side/dyrmgraph/data'
  extract_requested_columns(data_path, data_path, data_path, logging.DEBUG)
