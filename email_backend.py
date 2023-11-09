import json

from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

email_sent = set()

while True:
    for message in consumer:
        print("Processing Details...")
        details = json.loads(message.value.decode())
        print("Details: ", details)

        user_email = details["user_email"]
        email_sent.add(user_email)
        print(f"Email Sent to {user_email}. Total Email Sent: {len(email_sent)}.\n")