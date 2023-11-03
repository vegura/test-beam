from google.cloud import pubsub_v1
import random
import string
import time

project_id = "freedom-ukraine"
topic_name = "letter-number-input"

producer = pubsub_v1.PublisherClient()
topic_path = producer.topic_path(project_id, topic_name)

while True:
    message_data = random.choice(string.ascii_lowercase) + "," + str(random.randint(0,9))
    message = producer.publish(topic_path, data=message_data.encode("utf-8"))
    print(f"Published: {message_data}")
    time.sleep(random.uniform(0.1, 1.0))