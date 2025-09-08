import logging

from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from pyspark.sql import functions as F

# TODO: Match Docstrings with actual functionalities!


def norm_url(col):
  c = F.lower(col)
  c = F.regexp_replace(c, r'[?#].*$', '')
  return F.regexp_replace(c, r'/$', '')


def _join_tables(gkg: DataFrame, events: DataFrame, mentions: DataFrame):
  """
  - Takes the result data dict from the extract function
  - Joins and denormalizes into one table
  - Each row includes text to be embedded
  - Vector metadata from gcam, tone, etc columns
  """

  # TODO: EDA join method
  # mention:events = many:one
  # Mention-grain table
  m_e = (
    mentions.alias('m')
    .join(events.alias('e'), 'GLOBALEVENTID', 'left')
    # normalize before compositing
    .withColumn('doc_url', F.coalesce(F.col('m.doc_url_m'), F.col('e.SOURCEURL')))
    .withColumn('doc_url_norm', norm_url(F.col('doc_url')))
  )

  # join gkg
  g_m_e = m_e.join(gkg, 'doc_url_norm', 'left')

  # composit key hashing for exact dupe drop
  # TODO: This should be investigated ... Identifier as a key member? 🤔
  mention_key = [
    F.col('GLOBALEVENTID').cast('string'),
    F.col('MentionIdentifier'),
    F.col('SentenceID').cast('string'),
  ]

  g_m_e = g_m_e.withColumn('message_key', F.sha2(F.concat_ws('|', *mention_key), 256))

  w = W.partitionBy('message_key').orderBy(F.desc('V2_1DATE'))
  return g_m_e.withColumn('rn', F.row_number().over(w)).filter('rn=1').drop('rn')


# TODO: write, validate
def _sanitize_table(data: DataFrame):
  """
  - Recieves dat as Polars dataframe
  - Cleans the columns up removing things like offsets
  - Transform token-type columns
  """
  columns_to_drop = [F.col('doc_url'), F.col('doc_url_m')]
  dropped = data.drop(*columns_to_drop)
  # TODO:
  # sanitize GCAM and TONE as json
  # MAYBE save the one-table frame as parquet
  # structurize all other columns (as json or something: consider CPU load)
  # return it
  return dropped  # temporary


# TODO: validate
def transform(date: str, spark_config: dict, parquet_path: str) -> DataFrame:
  """
  - join and sanitize data using the helper functions
  - Sinks data to
  """
  from .utils import convert_date_to_partitions, open_spark_context

  # partition names
  dt, hr, qt = convert_date_to_partitions(date)

  # select the targeted date
  with open_spark_context(spark_config) as spark:
    g = spark.read.parquet(f'{parquet_path}_gkg').where(
      (F.col('dt') == dt) & (F.col('hr') == hr) & (F.col('qt') == qt)
    )
    e = spark.read.parquet(f'{parquet_path}_export').filter(
      (F.col('dt') == dt) & (F.col('hr') == hr) & (F.col('qt') == qt)
    )
    m = spark.read.parquet(f'{parquet_path}_mentions').filter(
      (F.col('dt') == dt) & (F.col('hr') == hr) & (F.col('qt') == qt)
    )

    # Filter gkg for web source
    g_web = g.filter(F.col('V2SOURCECOLLECTIONIDENTIFIER') == 1)

    # normalize url + gkg identifier has dupes
    g_uniq = (
      g_web.withColumn('doc_url_norm', norm_url(F.col('V2DOCUMENTIDENTIFIER')))
      .withColumn(
        'rn',
        F.row_number().over(
          W.partitionBy('doc_url_norm').orderBy(F.col('V2_1DATE').desc())
        ),
      )
      .filter('rn=1')
      .drop('rn')
    )

    # If a mentions record is not a url, then coalesce to the sourceurl column
    m_filtered = m.withColumn(
      'doc_url_m', F.when(F.col('MentionType') == 1, F.col('MentionIdentifier'))
    )

    as_one = _join_tables(g_uniq, e, m_filtered)
    sanitized = _sanitize_table(as_one)

  # TODO: Write a sync function <- wut
  return sanitized


def prune_columns(
  date: str,
  dic_path: str,
  request_path: str,
  spark_config: dict,
  csv: str,
  table: str,
  storage_path: str,
  parquet_dir: str,
  logger: logging.Logger,
  dic_name='column_dictionary.json',
  request_name='request_columns.json',
):
  """
  Column pruning + parquet writing
  """
  import json
  import os
  from datetime import datetime

  from pyspark.sql.functions import date_format, floor, lit, minute

  from .utils import open_spark_context

  with open(os.path.join(request_path, request_name)) as f:
    req: dict[str, list[str]] = json.load(f)
  with open(os.path.join(dic_path, dic_name)) as f:
    dic: dict[str, dict[str, str]] = json.load(f)

  with open_spark_context(spark_config) as spark:
    df = spark.read.option('sep', '\t').option('header', 'false').csv(csv)

    cols = dic[table]
    target_cols = req[table]

    # spark uses dot op to access columns
    whole = df.toDF(*[c.replace('.', '_') for c in cols.values()])  # label
    # target_df = whole.select(*target_cols)
    target_df = whole.select(*[c.replace('.', '_') for c in target_cols])

    dt = datetime.strptime(date, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

    target_df = (
      target_df.withColumn('dt', date_format(lit(dt), 'yyyy-MM-dd'))
      .withColumn('hr', date_format(lit(dt), 'HH'))
      .withColumn('qt', floor(minute(lit(dt)) / 15) * 15)
    )

    # Write to parquet
    parquet_path = os.path.join(storage_path, f'{parquet_dir}_{table}')
    os.makedirs(parquet_path, exist_ok=True)
    target_df.write.mode('overwrite').partitionBy('dt', 'hr', 'qt').parquet(
      parquet_path
    )
    logger.log(logger.level, msg=f'Pruning complete for table {table}')
