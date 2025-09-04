# Unit test for gdelt app
import asyncio
import os

import pytest

from services.cron.gdelt import gdelt as gd

# TODO: More coverage: currently only curates only the essentials


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

  # Could be the same as passing it as an arg..
  # but could be useful on an on-function-basis fixtures with autouse=False
  @pytest.mark.usefixtures('data_init_cleanup')
  def test_list_ready_files(self, gdelt_init_s2):
    import os

    from services.cron.gdelt.utils import list_ready_files

    gdelt = gdelt_init_s2()

    correct_names = [
      f'{gdelt.gdelt_config.local_dest}/{gdelt.target_date}.export.csv.zip',
      f'{gdelt.gdelt_config.local_dest}/{gdelt.target_date}.gkg.csv.zip',
      f'{gdelt.gdelt_config.local_dest}/{gdelt.target_date}.mentions.csv.zip',
    ]

    result = list_ready_files(gdelt.target_date, gdelt.gdelt_config.local_dest)
    assert set(result) == set(correct_names), f'BAD: {result}, GOOD: {correct_names}'
    assert os.path.exists(
      f'{gdelt.gdelt_config.local_dest}/{gdelt.target_date}{gdelt.gdelt_config.sentinel_name}'
    ), 'Must produce a sentinel file.'


# TODO: Schema test with regex
class TestExtraction:
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

  def test_extract(
    self,
    gdelt_init_s2,
  ):
    from helper import cleanup_data, handle_selector_jsons, init_data
    from pyspark.sql import SparkSession

    # Test in a separate folder for sanity 😩
    gdelt: gd.GDELT = gdelt_init_s2()
    test_path = '/lab/dee/repos_side/dyrmgraph/services/cron/tests'
    test_data_path = os.path.join(test_path, 'extract_test')
    os.makedirs(test_data_path, exist_ok=True)
    gdelt.gdelt_config.local_dest = test_data_path

    # Copy paste the column selector jsons
    handle_selector_jsons(
      test_path, True, source_path='/lab/dee/repos_side/dyrmgraph/data'
    )

    # init, extract, prune
    init_data(test_data_path, '20150218231500')
    gdelt.extract()

    with SparkSession.builder.getOrCreate() as spark:
      parquet_path = os.path.join(
        gdelt.gdelt_config.local_dest, gdelt.spark_config['parquet_dir']
      )
      test_g = spark.read.parquet(f'{parquet_path}_gkg').where('dt = "2015-02-18"')
      test_e = spark.read.parquet(f'{parquet_path}_export').filter('dt = "2015-02-18"')
      test_m = spark.read.parquet(f'{parquet_path}_mentions').filter(
        'dt = "2015-02-18"'
      )

      # Parquets must be there to make queries, hence it's before the cleanup.
      # _jdf exposes private attrib. NOPE
      gdelt.logger.log(
        gdelt.level,
        f"""
        -----gkg------
        {test_g.limit(5).toPandas().to_string()}
        ---mentions---
        {test_m.limit(5).toPandas().to_string()}
        ----export----
        {test_e.limit(5).toPandas().to_string()}""",
      )

    # TODO: data validation
    assert test_g is not None and test_e is not None and test_m is not None, (
      'All tables must be present and not empty'
    )

    cleanup_data(test_data_path)
    handle_selector_jsons(test_path, False)


class TestTransform:
  def test__sanitize_table(self):
    pass

  def test__join_tables(self):
    pass

  def test_transform(self):
    pass
