"""Kiểm tra producer/consumer Python với Kafka 4.x (kafka-python và confluent-kafka)."""
import json, sys, time, uuid
bootstrap = sys.argv[1] if len(sys.argv) > 1 else "kafka:9092"
topic = sys.argv[2] if len(sys.argv) > 2 else "kiem-tra"

from kafka import KafkaProducer, KafkaConsumer
p = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode())
for i in range(5):
    p.send(topic, {"src": "kafka-python", "i": i})
p.flush()
c = KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset="earliest",
                  group_id="kt-" + uuid.uuid4().hex[:6], consumer_timeout_ms=10000)
n = sum(1 for _ in c)
print("kafka-python consumed", n)

from confluent_kafka import Producer, Consumer
pr = Producer({"bootstrap.servers": bootstrap})
for i in range(5):
    pr.produce(topic, json.dumps({"src": "confluent-kafka", "i": i}).encode())
pr.flush()
co = Consumer({"bootstrap.servers": bootstrap, "group.id": "kt-" + uuid.uuid4().hex[:6],
               "auto.offset.reset": "earliest"})
co.subscribe([topic])
m, t0 = 0, time.time()
while time.time() - t0 < 15:
    msg = co.poll(1.0)
    if msg is not None and msg.error() is None:
        m += 1
co.close()
print("confluent-kafka consumed", m)
