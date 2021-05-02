import json
import threading
import time
import uuid

import schedule
from confluent_kafka import Consumer, Producer

from fetch_email_dict import count_unread_emails, get_emails
from kafka_topics import call_create_topic

BROKER_URL = "PLAINTEXT://localhost:9092"


def producer(email_list):
    p = Producer({"bootstrap.servers": BROKER_URL})

    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("\033[1;32;40mMessage delivered to {} [{}] \033[0;0m".format(msg.topic(), msg.partition()))

    for data in email_list:
        print('reached here')
        print(data)

        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        message_id = str(uuid.uuid4())
        data["Request_id"] = message_id

        # Asynchronously produce a message, the delivery report callback will be triggered from poll() above, or flush() below, when the message has been successfully delivered or failed permanently.
        p.produce("app-messages", value=bytes(str(data), "UTF-8"), on_delivery=delivery_report)
        print("\033[1;36;40m -- PRODUCER: Sent message with id {} \033[0;0m".format(message_id))

    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
    p.flush()


def consumer():

    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "email-categorisation",
            "auto.offset.reset": "earliest",
        }
    )

    c.subscribe(["app-messages"])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # print('Received message: {}'.format(msg.value().decode('utf-8')))
        print(
            "\033[1;32;40m ** CONSUMER: Received message: {}".format(
                msg.value().decode("utf-8")
            )
        )

    c.close()


def check_inbox():
    num_unread_msgs = count_unread_emails(["INBOX"])

    while num_unread_msgs != 0:
        messages = get_emails(["INBOX"])
        producer(messages)
        num_unread_msgs = count_unread_emails(["INBOX"])

messages = get_emails(["INBOX"])
producer(messages)

schedule.every(2).minutes.do(check_inbox)

while True:
    schedule.run_pending()
    time.sleep(1)

# def main():

# threads = []
# t = threading.Thread(target=producer)
# t2 = threading.Thread(target=consumer)
# threads.append(t)
# threads.append(t2)
# t.start()
# t2.start()

# producer()
# consumer()

if __name__ == "__main__":
    topics = ["app-messages", "retrain"]
    call_create_topic(client, topics)
