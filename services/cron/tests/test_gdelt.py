# Unit test for gdelt app
import asyncio
import os


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


class TestTransform:
  def test__sanitize_table(self):
    pass

  def test__join_tables(self):
    pass

  def test_transform(self):
    pass


# TODO: Utils must come first
class TestUtils:
  def test_logging(self):
    pass

  def test_closest_15min_before(self):
    """Tests the 15m interval getter"""

  def test_now(self):
    pass

  def test_get_url(self):
    pass

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
