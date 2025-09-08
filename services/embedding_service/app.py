import json
import os

import pgvector
import psycopg
import requests
from kafka import KafkaConsumer, KafkaProducer

TEI = os.getenv('TEI_BASE_URL', 'http://tei:80').rstrip('/')
conn = psycopg.connect(os.getenv('POSTGRES_CONN'), autocommit=True)
cur = conn.cursor()

producer = KafkaProducer(
  bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP'),
  value_serializer=lambda v: json.dumps(v).encode(),
)

consumer = KafkaConsumer(
  'gdelt.raw',
  bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP'),
  value_deserializer=lambda m: json.loads(m.decode()),
  auto_offset_reset='earliest',
  group_id='embedding_service',
)


def embed(text: str):
  # /embed is simpler > /v1/embeddings
  r = requests.post(f'{TEI}/embed', json={'inputs': text}, timeout=30)
  r.raise_for_status()
  vec = r.json()
  if isinstance(vec[0], float):  # single vector
    return vec
  return vec[0]


# TODO:
# Decode json columns from gdelt
# Inside Spark code, embed text: separation of concern does not look good here
# write to pgvector the id, vector, metadata(GCAM and TONE)
for msg in consumer:
  evt = msg.value
  text = ' '.join(
    filter(
      None,
      [
        evt.get('EventCode', ''),
        evt.get('Actor1Name', ''),
        evt.get('Actor2Name', ''),
        evt.get('ActionGeo_FullName', ''),
      ],
    )
  )
  vec = embed(text)
  cur.execute(
    """
    INSERT
    INTO events (gdelt_id, event_text, embedding, sql_date, actor1, actor2, source_url)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    RETURNING id, sql_date
    """,
    (
      evt.get('GlobalEventID'),
      text,
      pgvector.Vector(vec),
      evt.get('SQLDATE'),
      evt.get('Actor1Name'),
      evt.get('Actor2Name'),
      evt.get('SOURCEURL'),
    ),
  )
  row_id, sql_date = cur.fetchone()
  evt['embedding_row_id'] = row_id

  # for downstream graph pipeline
  producer.send('gdelt.embedded', evt)

  # publish a flat doc for els
  es_doc = {
    'index_name': 'gdelt-events',
    'doc_id': str(evt.get('GLOBALEVENTID')),  # stable id
    '@version': '1',
    'type': 'event',
    'gdelt_id': evt.get('GLOBALEVENTID'),
    'event_code': evt.get('EventCode'),
    'actor1': evt.get('Actor1Name'),
    'actor2': evt.get('Actor2Name'),
    'geo': evt.get('ActionGeo_FullName'),
    'source_url': evt.get('SOURCEURL'),
    'sql_date': evt.get('SQLDATE'),
    'text': text,
    'timestamp': evt.get('SQLDATE'),  # Logstash 'date' will map this into @timestamp
  }
  producer.send('gdelt.es_updates', es_doc)
