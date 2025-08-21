import json
import os
from hashlib import md5
from typing import TypedDict

import disinfo_labeler as dl
from kafka import KafkaConsumer, KafkaProducer
from langgraph.graph import END, StateGraph
from neo4j import GraphDatabase

# TODO: Type-narrow the envs, don't use default values
neo_driver = GraphDatabase.driver(
  os.getenv('NEO4J_URI', 'bolt://neo4j:7687'),
  auth=(os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password')),
)
producer = KafkaProducer(
  bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP'),
  value_serializer=lambda v: json.dumps(v).encode(),
)


class State(TypedDict):
  actor1: str
  actor2: str
  sql_date: str
  is_disinfo: bool
  source_url: str
  edge: dict[str, str]


# Reducer
def concept_node(state: State):
  a1 = (state['actor1'] or '').strip()
  a2 = (state['actor2'] or '').strip()
  if not a1 or not a2 or a1 == a2:  # self edges or empty
    state['is_disinfo'] = dl.label(state['source_url'])
    state['edge'] = {'from': a1, 'to': a2, 'day': state['sql_date']}
    return state

  c1, c2 = sorted([state['actor1'], state['actor2']])  # canonical co-occurence
  ts, url = state['sql_date'], state['source_url']

  with neo_driver.session() as s:
    s.run(
      """MERGE (n:Concept {name:$c}) 
            ON CREATE SET n.firstSeen:$ts 
            SET n.lastSeen:$ts""",
      c=c1,
      ts=ts,
    )
    s.run(
      """MERGE (n:Concept {name:$c}) 
            ON CREATE SET n.firstSeen:$ts 
            SET n.lastSeen:$ts""",
      c=c2,
      ts=ts,
    )
    s.run(
      """MATCH (a:Concept {name:$a}),(b:Concept {name:$b})
            MERGE (a)-[r:CO_OCCUR {day:$ts}]->(b)
            ON CREATE SET r.count=1
            ON MATCH  SET r.count=r.count+1""",
      a=c1,
      b=c2,
      ts=ts,
    )
  state['is_disinfo'] = dl.label(url)
  state['edge'] = {'from': c1, 'to': c2, 'day': ts}
  return state


g = StateGraph(State)
g.add_node('concept', concept_node)
g.set_entry_point('concept')
# TODO: Chain later with 'geo' or 'sentiment' etc... as needed
g.add_edge('concept', END)  # put 'concept' as the second arg for looping

compiled = g.compile()


def run():
  consumer = KafkaConsumer(
    'gdelt.embedded',
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP'),
    value_deserializer=lambda m: json.loads(m.decode()),
    group_id='langgraph',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=0,
  )
  for msg in consumer:
    # NOTE: If we're gonna mutate the msg.value anyway,
    # what's the point in passing a clean state?
    # -> msg.value contains more keys than the state
    state: State = {
      'actor1': msg.value.get('actor1') or msg.value.get('Actor1Name'),
      'actor2': msg.value.get('actor2') or msg.value.get('Actor2Name'),
      'sql_date': msg.value.get('sql_date') or msg.value.get('SQLDATE'),
      'is_disinfo': False,
      'source_url': msg.value.get('source_url') or msg.value.get('SOURCEURL'),
      'edge': {'from': '', 'to': '', 'day': ''},
    }
    final = compiled.invoke(state)
    enriched = dict(msg.value)
    enriched['is_disinfo'] = final['is_disinfo']
    producer.send('gdelt.graph_updates', enriched)

    # graph-edge doc for es
    edge = final['edge']
    edge_id = md5(f'{edge["from"]}|{edge["to"]}|{edge["day"]}'.encode()).hexdigest()
    es_graph_doc = {
      'index_name': 'gdelt-graph',
      'doc_id': edge_id,
      'edge_id': edge_id,
      '@version': '1',
      'type': 'graph_edge',
      'from': edge['from'],
      'to': edge['to'],
      'count_increment': 1,
      'is_disinfo': final['is_disinfo'],
      'edge_timestamp': edge['day'],  # Logstash date filter maps this to @timestamp
    }
    producer.send('gdelt.es_updates', es_graph_doc)


if __name__ == '__main__':
  run()
