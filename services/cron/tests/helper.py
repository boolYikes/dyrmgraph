"""
Helper functions for tests
"""

# Func name must not start or end with test
import os
import shutil


def cleanup_data(directory):
  shutil.rmtree(directory)


def init_data(directory: str, date: str):
  from concurrent.futures import ThreadPoolExecutor
  from urllib.request import urlretrieve

  os.makedirs(directory, exist_ok=True)
  baseurl = 'http://data.gdeltproject.org/gdeltv2/'
  files = [
    f'{date}.gkg.csv.zip',
    f'{date}.export.CSV.zip',
    f'{date}.mentions.CSV.zip',
  ]

  def _retrieve(baseurl, f):
    url = os.path.join(baseurl, f)
    urlretrieve(url, os.path.join(directory, f.lower()))

  with ThreadPoolExecutor(max_workers=3) as exec:
    for f in files:
      exec.submit(_retrieve, baseurl, f)

  with open(os.path.join(directory, 'done_list.txt'), 'w') as f:
    f.write(f'{date}\n')

  open(os.path.join(directory, f'{date}.gdelt_batch_complete'), 'w').close()


def handle_selector_jsons(target_path, is_init: bool, source_path=''):
  """Copies the column selectors if init else clean them up"""
  target_files = ['request_columns.json', 'column_dictionary.json']
  for f in target_files:
    if is_init:
      shutil.copy(os.path.join(source_path, f), os.path.join(target_path, f))
    else:
      os.remove(os.path.join(target_path, f))
