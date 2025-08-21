import os

import pgvector
import psycopg
import requests
from fastapi import FastAPI, Query
from neo4j import GraphDatabase
from openai import OpenAI

TEI = os.getenv('TEI_BASE_URL', 'http://tei:80').rstrip('/')
pg_conn = psycopg.connect(os.getenv('POSTGRES_CONN'), autocommit=True)
neo = GraphDatabase.driver(
  os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
)

client = OpenAI(
  base_url=os.getenv('OPENAI_BASE_URL', 'http://llm:8082/v1'),
  api_key=os.getenv('OPENAI_API_KEY', 'sk-local'),
)

app = FastAPI(title='GDELT RAG API LOCAL')


def embed_query(q: str):
  r = requests.post(f'{TEI}/embed', json={'inputs': q}, timeout=30)
  r.raise_for_status()
  vec = r.json()
  return vec if isinstance(vec[0], float) else vec[0]


@app.get('/ask')
def ask(question: str = Query(..., min_length=5, max_length=300)):
  vec = embed_query(question)
  cur = pg_conn.cursor()
  cur.execute(
    """
    SELECT id, gdelt_id, event_text, sql_date, actor1, actor2, source_url
    FROM events
    ORDER BY embedding <=> %s
    LIMIT 5
    """,
    (pgvector.Vector(vec),),
  )
  rows = cur.fetchall()

  # concept neighbors (demo)
  with neo.session() as s:
    # use actor names collected from rows as "concepts" to fetch neighbors
    actors = list({r[4] for r in rows if r[4]} | {r[5] for r in rows if r[5]})
    neighbors = s.run(
      """
      MATCH (a:Concept)-[r:CO_OCCUR]->(b:Concept)
      WHERE a.name IN $actors
      RETURN DISTINCT b.name LIMIT 12
    """,
      actors=actors,
    ).value()

  facts = '\n'.join([f'- {r[2]} ({r[6]})' for r in rows])
  system = (
    "Use ONLY the facts below. If insufficient, say you don't know. "
    'Cite the provided URLs inline.'
  )
  user = (
    f'Question: {question}\nFacts:\n{facts}\nRelated concepts: {", ".join(neighbors)}'
  )
  completion = client.chat.completions.create(
    model='local-gguf',
    messages=[{'role': 'system', 'content': system}, {'role': 'user', 'content': user}],
    temperature=0.2,
    max_tokens=400,
  )

  return {
    'answer': completion.choices[0].message.content,
    'sources': [r[6] for r in rows],
  }
