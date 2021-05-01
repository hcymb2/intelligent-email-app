import pandas as pd
import json
import threading
import uuid

from pathlib import Path
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
from fetch_email_dict import get_emails



KAFKA_HOST = 'localhost:9092'

### poll GMAIL repeatedly
messages = get_emails("INBOX")

def start_producing():
	producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
	for i in len(messages):
		message_id = str(uuid.uuid4())
		message = {'request_id': message_id, 'data': json.loads(messages[i])}
		message_value = json.dumps(message).encode('utf-8')

		producer.send('app_messages', json.dumps(message).encode('utf-8'))
		producer.flush()

		print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
		sleep(2)

def start_consuming():
	consumer = KafkaConsumer('app_messages', bootstrap_servers=KAFKA_HOST)

	for msg in consumer:
		message = json.loads(msg.value)
		if 'prediction' in message:
			request_id = message['request_id']
			print("\033[1;32;40m ** CONSUMER: Received prediction {} for request id {}".format(message['prediction'], request_id))


threads = []
t = threading.Thread(target=start_producing)
t2 = threading.Thread(target=start_consuming)
threads.append(t)
threads.append(t2)
t.start()
t2.start()
