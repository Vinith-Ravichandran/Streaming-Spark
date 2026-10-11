import json
import time
from confluent_kafka import Producer
from mock_data_generator import generate_transaction

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'retail-producer'
}

producer = Producer(conf)
topic = 'retail-transactions'


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Produced -> Key: {msg.key().decode('utf-8')} | Topic: {msg.topic()} | Partition: [{msg.partition()}] | Offset: {msg.offset()}")


print(f"Streaming data to '{topic}' on localhost:9092. Press Ctrl+C to stop...\n")

try:
    while True:
        record = generate_transaction()
        payload = json.dumps(record).encode('utf-8')

        # Partitioning by customer_id
        producer.produce(
            topic=topic,
            key=record["customer_id"].encode('utf-8'),
            value=payload,
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1.0)  # Emits 1 event per second
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()