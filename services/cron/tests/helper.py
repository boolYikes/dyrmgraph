def cleanup_data(directory):
  import os

  files = os.listdir(directory)
  for f in files:
    os.remove(os.path.join(directory, f))
