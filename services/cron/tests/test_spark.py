from services.cron.gdelt.gdelt import GDELT


class TestSpark:
  def test_spark_connection(self, gdelt_init_s2):
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession

    gd: GDELT = gdelt_init_s2()

    conf = SparkConf().setAll(
      [
        gd.spark_config['master'],
        gd.spark_config['app_name'],
        gd.spark_config['enable_log'],
        gd.spark_config['memory'],
      ]
    )
    with SparkSession.builder.config(conf=conf).getOrCreate() as ss:
      assert ss is not None, 'Must be able to create a spark session'

  # TODO: How to assert?
  # TODO: Need join/sanitize separately?
  def test_transform(self, gdelt_init_s2):
    import os

    from helper import cleanup_data, handle_selector_jsons, init_data

    from services.cron.gdelt.transform import transform
    from services.cron.gdelt.utils import open_spark_context

    gd: GDELT = gdelt_init_s2()
    test_path = '/lab/dee/repos_side/dyrmgraph/services/cron/tests'
    test_data_path = os.path.join(test_path, 'extract_test')
    os.makedirs(test_data_path, exist_ok=True)
    gd.gdelt_config.local_dest = test_data_path

    handle_selector_jsons(
      test_path, True, source_path='/lab/dee/repos_side/dyrmgraph/data'
    )

    # init, extract, prune
    init_data(test_data_path, '20150218231500')

    with open_spark_context(dict(gd.spark_config)) as spark:
      gd.extract(spark)

      # transform
      parquet_path = os.path.join(
        gd.gdelt_config.local_dest, gd.spark_config['parquet_dir']
      )
      df = transform(gd.target_date, spark, parquet_path)

      # DEBUG
      summary_str = df.describe().toPandas().to_string()
      gd.logger.debug(summary_str)
      # df.toLocalIterator()

    # Placeholder
    assert df is not None, 'Must not be null'

    cleanup_data(test_data_path)
    handle_selector_jsons(test_path, False)
