import logging

import polars as pl
from pyspark.sql import DataFrame, SparkSession


# TODO: Convert to Spark
def _join_tables(data: DataFrame):
  """
  - Takes the result data dict from the extract function
  - Joins and denormalizes into one table
  - Each row includes text to be embedded
  - Vector metadata from gcam, tone, etc columns
  """
  gkg, events, mentions = data['gkg'], data['export'], data['mentions']
  g = pl.Dataframe(gkg)
  e = pl.Dataframe(events)
  m = pl.Dataframe(mentions)
  # Mention-grain table, so Mentions-Events-left-join
  m_e = m.join(e, on='GlobalEventID', how='left')
  # doc_url: MentionIdentifier (web) else SOURCEURL from Events
  m_e = m_e.with_columns(
    pl.when(pl.col('MentionType') == 1)
    .then(pl.col('MentionIdentifier'))
    .otherwise(pl.col('SOURCEURL'))
    .alias('doc_url')
  )
  # gkg prepping, web source only
  g1 = (
    g.filter(pl.col('V2SOURCECOLLECTIONIDENTIFIER') == 1)
    .with_columns(pl.col('V2DOCUMENTIDENTIFIER').alias('doc_url'))
    .select(
      [
        'doc_url',
        'V2SOURCECOMMONNAME',
        'V2.1DATE',
        'V2ENHANCEDTHEMES',
        'V2ENHANCEDLOCATIONS',
        'V2ENHANCEDPERSONS',
        'V2ENHANCEDORGANIZATIONS',
        'V1.5TONE',
        'V2GCAM',
        'V2.1QUOTATIONS',
        'V2.1ALLNAMES',
        'V2.1AMOUNTS',
      ]
    )
  )

  res: DataFrame = m_e.join(g1, on='doc_url', how='left').unique(
    subset=['GlobalEventID', 'doc_url', 'SentenceID']
  )
  return res


# TODO: write, validate
def _sanitize_table(data: DataFrame):
  """
  - Recieves dat as Polars dataframe
  - Cleans the columns up removing things like offsets
  - Transform token-type columns
  """
  return data  # placeholder


# TODO: write, validate
def transform(date: str) -> DataFrame:
  """
  - join and sanitize data using the helper functions
  - Sinks data to
  """
  # as_one = _join_tables(data)
  # sanitized = _sanitize_table(as_one)
  ...
  # TODO: Write a sync function
  # return sanitized


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

  from pyspark.conf import SparkConf
  from pyspark.sql.functions import date_format, floor, lit, minute

  with open(os.path.join(request_path, request_name)) as f:
    req: dict[str, list[str]] = json.load(f)
  with open(os.path.join(dic_path, dic_name)) as f:
    dic: dict[str, dict[str, str]] = json.load(f)

  conf = SparkConf().setAll(
    [
      spark_config['master'],
      spark_config['app_name'],
      spark_config['enable_log'],
      spark_config['memory'],
    ]
  )
  with SparkSession.builder.config(conf=conf).getOrCreate() as spark:
    df = spark.read.option('sep', '\t').option('header', 'false').csv(csv)

    cols = dic[table]
    target_cols = req[table]

    whole = df.toDF(*[c.replace('.', '_') for c in cols.values()])  # label
    # target_df = whole.select(*target_cols)  # Literal periods don't work
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
