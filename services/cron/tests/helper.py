def cleanup_data(directory):
  import os

  files = os.listdir(directory)
  for f in files:
    os.remove(os.path.join(directory, f))


def init_data(directory: str, date: str):
  import os
  from urllib.request import urlretrieve

  os.makedirs(directory, exist_ok=True)
  baseurl = 'http://data.gdeltproject.org/gdeltv2/'
  files = [
    f'{date}.gkg.csv.zip',
    f'{date}.export.CSV.zip',
    f'{date}.mentions.CSV.zip',
  ]
  for f in files:
    url = os.path.join(baseurl, f)
    urlretrieve(url, os.path.join(directory, f.lower()))

  with open(os.path.join(directory, 'done_list.txt'), 'w') as f:
    f.write(f'{date}\n')

  open(os.path.join(directory, f'{date}.gdelt_batch_complete'), 'w').close()
