import json

from kafka import KafkaProducer, KafkaConsumer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:29092"
)

while True:
    for message in consumer:
        print("Processing Order...")
        order_details = json.loads(message.value.decode())
        print("Order Details: ", order_details)

        order_id = order_details["order_id"]
        user_id = order_details["user_id"]
        cost = order_details["cost"]

        order_data = {
            "user_id": user_id,
            "user_email": f"{user_id}@gmail.com",
            "order_total": cost
        }

        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(order_data).encode('utf-8')
        )
        print(f"Order {order_id} Confirmed.\n")
