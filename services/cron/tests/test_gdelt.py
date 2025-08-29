# Unit test for gdelt app
import asyncio
import os

import pytest

# TODO: More coverage


class TestUtils:
  def test_closest_15min_before(self):
    """Tests the 15m interval getter"""
    from services.cron.gdelt.utils import closest_15min_before

    result = closest_15min_before('20150218231520')
    assert result == '20150218231500', f'Should be 20150218231500, not {result}'

  def test_get_url(self):
    from services.cron.gdelt.utils import get_url

    test_date_now = '20150218231520'
    actual_date = '20150218231500'
    urls, filenames, target_date = get_url(test_date_now)
    BASE_URL = 'http://data.gdeltproject.org/gdeltv2/'
    correct_urls = [
      f'{BASE_URL}{actual_date}.gkg.csv.zip',
      f'{BASE_URL}{actual_date}.mentions.CSV.zip',
      f'{BASE_URL}{actual_date}.export.CSV.zip',
    ]
    correct_filenames = [
      f'{actual_date}.gkg.csv.zip',
      f'{actual_date}.mentions.CSV.zip',
      f'{actual_date}.export.CSV.zip',
    ]
    assert urls == correct_urls, f'BAD: {urls}, GOOD: {correct_urls}'
    assert filenames == correct_filenames, (
      f'BAD: {filenames}, GOOD: {correct_filenames}'
    )
    assert target_date == actual_date, f'BAD: {target_date}, GOOD: {actual_date}'

  @pytest.mark.usefixtures(
    'data_init_cleanup'
  )  # I think it's the same as passing it as an arg
  def test_list_ready_files(self, gdelt_init_s2):
    import asyncio
    import os

    from services.cron.gdelt.utils import list_ready_files

    gdelt = gdelt_init_s2()

    correct_names = asyncio.run(gdelt.ingest())

    result = list_ready_files(
      '20150218231500', '/lab/dee/repos_side/dyrmgraph/data/samples'
    )
    assert set(result) == set(correct_names), f'BAD: {result}, GOOD: {correct_names}'
    assert os.path.exists(
      '/lab/dee/repos_side/dyrmgraph/data/samples/20150218231500.gdelt_batch_complete'
    ), 'Must produce a sentinel file.'

  def test_get_columns(self):
    pass

  def test_is_done_with_ingestion(self):
    pass

  def test_clean_up_ingestion(self):
    pass


class TestTransform:
  def test__sanitize_table(self):
    pass

  def test__join_tables(self):
    pass

  def test_transform(self):
    pass


class TestGDELT:
  # TODO: Placeholder. Convert it to a schema test with regex
  # That fixture is tricky to type
  def test_init(self, gdelt_init_s1, gdelt_init_s2):
    gdelt = gdelt_init_s2()
    assert gdelt.gdelt_config.date == gdelt_init_s1['date'], 'Should be True'

  def test_ingest(self, gdelt_init_s2):
    from services.cron.tests.helper import cleanup_data

    gdelt = gdelt_init_s2()
    file_list = asyncio.run(gdelt.ingest())
    assert len(file_list) == 3 and os.path.exists(
      os.path.join(gdelt.gdelt_config.local_dest, '20150218231500.gdelt_batch_complete')
    ), 'There should be 3 files ready and *.gdelt_batch_complete must be there'
    cleanup_data(gdelt.gdelt_config.local_dest)

  def test_extract(self):
    pass

  def test_run(self):
    """Tests the run from __main__"""
    pass
