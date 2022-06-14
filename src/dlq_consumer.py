import json
import traceback

from confluent_kafka import Consumer

# config
BOOTSTRAP_SERVERS = "192.168.10.243:19092"
topic = "dlq"

c = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "group-dlq",
        "auto.offset.reset": "earliest",
    }
)

c.subscribe([topic])

while True:
    msg = c.poll(0.1)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print("Recieved header: {}".format(msg.headers()))
    print("Received value: {}".format(msg.value().decode("utf-8")))

c.close()
