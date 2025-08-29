class TestKafka:
  def test_init(self, topic):
    assert topic == 'test.raw'

  def test_pubsub(self, init_pub_sub, topic):
    producer, consumer = init_pub_sub
    producer.send(topic, key='k1-test', value='v1-test')
    producer.flush()

    records = []
    for msg in consumer.poll(timeout_ms=1500).values():
      records.extend(msg)
    assert any(r.value == 'v1-test' for r in records)
