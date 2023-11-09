import json

from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

orders_count = 0
total_revenue = 0

while True:
    for message in consumer:
        print("Processing Details...")
        details = json.loads(message.value.decode())
        print("Details: ", details)

        order_cost = details["order_total"]
        total_revenue += order_cost
        orders_count += 1

        print(f"Total Order: {orders_count}; Total Revenue: {total_revenue}.\n")