class TestSpark:
  def test_spark_connection(self, gdelt_init_s2):
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession

    gd = gdelt_init_s2()

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

  def test_spark_IO(self, gdelt_init_s2): ...
