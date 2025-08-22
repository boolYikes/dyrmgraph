import polars as pl


# TODO: Validate
# NOTE: Lightweight Polars solution. Move to Spark if it goes distributed
def _join_tables(data: dict[str, list[dict[str, str]]]):
  """
  - Takes the result data dict from the extract function
  - Joins and denormalizes into one table
  - Each row includes text to be embedded
  - Vector metadata from gcam, tone, etc columns
  """
  gkg, events, mentions = data['gkg'], data['events'], data['mentions']
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

  res: pl.DataFrame = m_e.join(g1, on='doc_url', how='left').unique(
    subset=['GlobalEventID', 'doc_url', 'SentenceID']
  )
  return res


# TODO: write, validate
def _sanitize_table(data: pl.DataFrame):
  """
  - Recieves dat as Polars dataframe
  - Cleans the columns up removing things like offsets
  - Transform token-type columns
  """
  return data  # placeholder


# TODO: write, validate
def transform(data: dict[str, list[dict[str, str]]]) -> pl.DataFrame:
  """
  - join and sanitize data using the helper functions
  - Sinks data to
  """
  as_one = _join_tables(data)
  sanitized = _sanitize_table(as_one)

  # TODO: Write a sync function
  return sanitized
