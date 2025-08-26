class TestKafka:
  def test_init(self, topic):
    assert topic == 'test.raw'
