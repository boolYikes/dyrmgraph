import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window as W
from pyspark.sql import functions as F
from pyspark.sql import types as T

# TODO: Match Docstrings with actual functionalities!
# TODO: Precarious jsonificiation. Be defensive
# -> If GDELT honours safe split like python -> no need to defend


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

  # TODO: Aliases will mess about the dupe columns.
  # -> Deal with it!
  # mention:events = many:one
  # Mention-grain table
  m_e = (
    mentions.alias('m')
    .drop('hr')
    .drop('dt')
    .drop('qt')
    .join(events.alias('e').drop('hr').drop('dt').drop('qt'), 'GLOBALEVENTID', 'left')
    # normalize before compositing
    .withColumn('doc_url', F.coalesce(F.col('m.doc_url_m'), F.col('e.SOURCEURL')))
    .withColumn('doc_url_norm', norm_url(F.col('doc_url')))
  )

  # join gkg
  g_m_e = m_e.join(gkg.drop('hr').drop('dt').drop('qt'), 'doc_url_norm', 'left')

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


@F.udf(T.MapType(T.StringType(), T.DoubleType()))
def _map_gkg_tone(s: str):
  """
  V1.5TONE is comma separated
  6 dimensions + wordcount
  1. Tone: -100 ~ 100 (Positive - Negative) -> ignore for headroom?
  2. Positive score: 0 ~ 100 -> percentage of words that have positive connotation
  3. Negative score: 0 ~ 100 -> percentage of words that have neagtive connotation
  4. Polarity: float. 0 ~ 100 -> percentage of emotionally charged words
  5. Activity reference density: float. -> percentage of words that represent activity
  6. Self/Group reference density: float -> percentage of things like pronoun  usage
  7. word count: int
  """
  if not s:
    return {}

  out: dict[str, int | float] = {}
  dims = s.split(',')
  try:
    out['tone'] = int(dims[0])
    out['positive'] = int(dims[1])
    out['negative'] = int(dims[2])
    out['polarity'] = float(dims[3])
    out['activity'] = float(dims[4])
    out['reference'] = float(dims[5])
    out['wc'] = int(dims[6])
  except BaseException as e:
    raise BaseException from e
  return out


# Use `python services/cron/gdelt/utils.py to query by dictionary IDs e.g., c10.1`
@F.udf(T.MapType(T.StringType(), T.DoubleType()))
def _map_gkg_gcam(s: str):
  """A UDF for mapping GCAM column"""
  if not s:
    return {}
  out = {}
  for tok in s.split(','):
    tok = tok.strip()
    if not tok:
      continue
    key, val = tok.split(':')
    try:
      out[key] = float(val)
    except BaseException as e:
      raise BaseException from e
  return out


@F.udf(T.ArrayType(T.MapType(T.StringType(), T.StringType())))
def _map_gkg_counts(s: str):
  """Enhanced. Uses offset. Returned as list"""
  if not s:
    return []
  out: list[dict] = []
  for rec in s.split(';'):
    rec = rec.strip()
    if not rec:
      continue
    parts = rec.split('#')
    new_rec = {}
    try:
      if len(parts) == 11:
        (
          new_rec['cnt_type'],
          new_rec['cnt'],
          new_rec['obj_type'],
          new_rec['loc_type'],
          new_rec['loc_fullname'],
          new_rec['loc_countrycode'],
          new_rec['loc_adm1code'],
          new_rec['loc_latitude'],
          new_rec['loc_longitude'],
          new_rec['loc_featureid'],
          new_rec['offset'],
        ) = parts
        out.append(new_rec)
      else:  # TODO: Defensive handling needed regardless of homogeniety
        ...
    except BaseException as e:
      raise BaseException from e
  return out


@F.udf(T.ArrayType(T.MapType(T.StringType(), T.StringType())))
def _map_gkg_location(s: str):
  """Enhanced. Uses offset. Returned as list"""
  if not s:
    return []
  out: list[dict] = []

  for rec in s.split(';'):
    rec = rec.strip()
    if not rec:
      continue
    dims = rec.split('#')
    # NOTE: must use location featureID to tell uniques
    # NOTE: BUT there's a danger that the featureID could be empty (??)
    # NOTE: The original is sorted by offset, ascending.
    # -> using list to preserve order, I can map in later stage
    # TODO: Although the records seem homogeneous, must be more defensive
    # -> null check + handling
    new_rec = {}
    (
      new_rec['type'],  # int
      new_rec['full_name'],  # txt
      new_rec['country_code'],  # txt
      new_rec['adm1'],  # txt
      new_rec['adm2'],  # txt
      new_rec['lat'],  # float
      new_rec['lon'],  # float
      new_rec['feature_id'],  # text | signed int -> empty (?) if country name or ADM1
      new_rec['offset'],  # int
    ) = dims
    out.append(new_rec)

  return out


@F.udf(T.ArrayType(T.MapType(T.StringType(), T.IntegerType())))
def _map_gkg_enhanced_v1(s: str):
  """Reusable by THEMES/PERSONS/ORGANIZATIONS/ALLNAMES/AMOUNTS only."""
  if not s:
    return []
  out: list[dict] = []
  for rec in s.split(';'):
    rec = rec.strip()
    if not rec:
      continue
    rec_split = rec.split(',')

    match rec_split:
      case [name, offset]:
        out.append({'name': name, 'offset': offset})
      case [amount, obj_name, offset]:
        out.append({'amount': amount, 'offset': offset, 'name': obj_name})
      case _:
        raise BaseException('Record dimension does not fit')

  return out


@F.udf(T.ArrayType(T.MapType(T.StringType(), T.StringType())))
def _map_gkg_dates(s: str):
  if not s:
    return []
  out: list[dict] = []

  for rec in s.split(';'):
    if not rec:
      continue
    new_rec = {}
    (
      new_rec['resolution'],
      new_rec['month'],
      new_rec['day'],
      new_rec['year'],
      new_rec['offset'],
    ) = rec.split(',')

    out.append(new_rec)

  return out


@F.udf(T.ArrayType(T.MapType(T.StringType(), T.StringType())))
def _map_gkg_quotes(s: str):
  if not s:
    return []
  out: list[dict] = []

  for rec in s.split('#'):
    new_rec = {}
    (
      new_rec['offset'],  # int
      new_rec['length'],  # int
      new_rec['verb'],  # str
      new_rec['quote'],  # str
    ) = rec.split('|')

    out.append(new_rec)

  return out


# TODO: write, validate
def _sanitize_table(data: DataFrame):
  """
  - Cleans the columns up removing things like offsets
  - Transform token-type columns
  """
  # Drop redundant columns
  columns_to_drop = [F.col('doc_url'), F.col('doc_url_m')]
  dropped = data.drop(*columns_to_drop)

  # sanitize GCAM and TONE as json
  tokens_cleaned = (
    dropped.withColumn('gcam_map', _map_gkg_gcam(F.col('V2GCAM')))
    .withColumn('tone_map', _map_gkg_tone(F.col('V1_5TONE')))
    .drop(F.col('V2GCAM'))
    .drop(F.col('V1_5TONE'))
  )  # clean up

  # structurize all other columns (as json: consider overhead)
  # NOTE: only GKG has nested ones.
  structurized = (
    tokens_cleaned.withColumn('counts', _map_gkg_counts('V2_1COUNTS'))
    .withColumn('themes', _map_gkg_enhanced_v1('V2ENHANCEDTHEMES'))
    .withColumn('location', _map_gkg_location('V2ENHANCEDLOCATIONS'))
    .withColumn('persons', _map_gkg_enhanced_v1('V2ENHANCEDPERSONS'))
    .withColumn('organizations', _map_gkg_enhanced_v1('V2ENHANCEDORGANIZATIONS'))
    .withColumn('dates', _map_gkg_dates('V2_1ENHANCEDDATES'))
    .withColumn('quotes', _map_gkg_quotes('V2_1QUOTATIONS'))
    .withColumn('allnames', _map_gkg_enhanced_v1('V2_1ALLNAMES'))
    .withColumn('amounts', _map_gkg_enhanced_v1('V2_1AMOUNTS'))
    .drop(F.col('V2_1COUNTS'))
    .drop(F.col('V2ENHANCEDTHEMES'))
    .drop(F.col('V2ENHANCEDLOCATIONS'))
    .drop(F.col('V2ENHANCEDPERSONS'))
    .drop(F.col('V2ENHANCEDORGANIZATIONS'))
    .drop(F.col('V2_1ENHANCEDDATES'))
    .drop(F.col('V2_1QUOTATIONS'))
    .drop(F.col('V2_1ALLNAMES'))
    .drop(F.col('V2_1AMOUNTS'))
  )

  # >> Don't infer schema later, use defined schema <<
  return structurized


# TODO: validate
def transform(date: str, spark: SparkSession, parquet_path: str) -> DataFrame:
  """
  - join and sanitize data using the helper functions
  - Sinks data to
  """
  from .utils import convert_date_to_partitions

  # partition names
  dt, hr, qt = convert_date_to_partitions(date)

  # select the targeted date
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
  spark_session: SparkSession,
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

  with open(os.path.join(request_path, request_name)) as f:
    req: dict[str, list[str]] = json.load(f)
  with open(os.path.join(dic_path, dic_name)) as f:
    dic: dict[str, dict[str, str]] = json.load(f)

  df = spark_session.read.option('sep', '\t').option('header', 'false').csv(csv)

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
  target_df.write.mode('overwrite').partitionBy('dt', 'hr', 'qt').parquet(parquet_path)
  logger.log(logger.level, msg=f'Pruning complete for table {table}')
