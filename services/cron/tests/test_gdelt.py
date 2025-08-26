# Unit test for gdelt app
import asyncio
import os

# TODO: More coverage


class TestUtils:
  def test_closest_15min_before(self):
    """Tests the 15m interval getter"""
    from services.cron.gdelt.utils import closest_15min_before

    result = closest_15min_before('20150218231520')
    assert result == '20150218231500', f'Should be 20150218231500, not {result}'

  def test_get_url(self):
    from services.cron.gdelt.utils import get_url

    urls, filenames, target_date = get_url('20150218231520')
    BASE_URL = 'http://data.gdeltproject.org/gdeltv2/'
    correct_urls = [
      f'{BASE_URL}20150218231500.gkg.csv.zip',
      f'{BASE_URL}20150218231500.mentions.CSV.zip',
      f'{BASE_URL}20150218231500.export.CSV.zip',
    ]
    correct_filenames = [
      '20150218231500.gkg.csv.zip',
      '20150218231500.mentions.CSV.zip',
      '20150218231500.export.CSV.zip',
    ]
    correct_target_date = '20150218231500'
    assert urls == correct_urls, f'BAD: {urls}, GOOD: {correct_urls}'
    assert filenames == correct_filenames, (
      f'BAD: {filenames}, GOOD: {correct_filenames}'
    )
    assert target_date == correct_target_date, (
      f'BAD: {target_date}, GOOD: {correct_target_date}'
    )

  def test_list_ready_files(self):
    pass

  def test_add_to_done_list(self):
    pass

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
    assert gdelt_init_s2.gdelt_config.date == gdelt_init_s1['date'], 'Should be True'

  def test_ingest(self, gdelt_init_s2):
    file_list = asyncio.run(gdelt_init_s2.ingest())
    assert len(file_list) == 3 and os.path.exists(
      os.path.join(
        gdelt_init_s2.gdelt_config.local_dest, '20150218231500.gdelt_batch_complete'
      )
    ), 'There should be 3 files ready and *.gdelt_batch_complete must be there'

  def test_extract(self):
    pass

  def test_run(self):
    """Tests the run from __main__"""
    pass
