import json
import threading
import uuid
from time import sleep

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from fetch_email_dict import get_emails

from kafka_topics import create_topic, topic_exists

BROKER_URL = "PLAINTEXT://localhost:9092"
client = AdminClient({"bootstrap.servers": BROKER_URL})

### poll GMAIL repeatedly
messages = get_emails("INBOX")

def producer():
    p = Producer({"bootstrap.servers": BROKER_URL})

    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    for data in messages:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        message_id = str(uuid.uuid4())
        message = {'request_id': message_id, 'data': data}

        # Asynchronously produce a message, the delivery report callback will be triggered from poll() above, or flush() below, when the message has been successfully delivered or failed permanently.
        p.produce('app-messages', value=str(message), on_delivery=delivery_report)
        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        sleep(2)

    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
    p.flush()


def consumer():

    c = Consumer({
        'bootstrap.servers': BROKER_URL,
        'group.id': 'email-categorisation',
        'auto.offset.reset': 'earliest'
    })

    c.subscribe(['app-messages'])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        #print('Received message: {}'.format(msg.value().decode('utf-8')))
        print("\033[1;32;40m ** CONSUMER: Received message: {}".format(msg.value().decode('utf-8')))

    c.close()



def call_create_topic(topics):
    """checks if topic exists, if doesn't, it calls create topics function
    
    Parameters:
    ----------
    topics : list of topics

    """
    for topic_name in topics:
        exists = topic_exists(client, topic_name)
        print(f"Topic {topic_name} exists: {exists}")

        if exists is False:
            create_topic(client, topic_name)
        else:
            print("Topic already exists")
            


def main():
    """Checks for topic and creates the topic if it does not exist"""

    

    topics = ['app-messages', 'retrain']
    call_create_topic(topics)

    # threads = []
    # t = threading.Thread(target=producer)
    # t2 = threading.Thread(target=consumer)
    # threads.append(t)
    # threads.append(t2)
    # t.start()
    # t2.start()

    producer()
    consumer()


if __name__ == "__main__":
    main()
