import json

from confluent_kafka import Consumer, KafkaException

from producer import value

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id":"order-tracker'",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])
print("consumer is running and listening the topic...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"error: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"Order {order['item']} with quantity {order['quantity']} by {order['user']}")
except KafkaException:
    print("Stopping consumer")

finally:
    consumer.close()