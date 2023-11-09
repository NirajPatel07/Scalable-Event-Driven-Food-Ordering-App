import json
import random
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")

for id in range(1, ORDER_LIMIT+1):
    order_details = {
        "order_id": id,
        "user_id": f"user_{id}",
        "cost": id * random.randint(1, 100)
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(order_details).encode('utf-8')
    )

    print(f"Order {id} Processing...\n")
    time.sleep(10)
