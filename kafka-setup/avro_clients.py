import json
import threading
import time
import uuid

import schedule
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from fetch_email_dict import count_unread_emails, get_emails
from kafka_topics import call_create_topic

BROKER_URL = "PLAINTEXT://localhost:9092"


def avro_producer(email_list):

    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("\033[1;32;40mMessage delivered to {} [{}] \033[0;0m".format(msg.topic(), msg.partition()))


    avroProducer = AvroProducer(
        {
            "bootstrap.servers": BROKER_URL,
            "on_delivery": delivery_report,
            "schema.registry.url": "http://localhost:8081/",
        },
        default_value_schema=value_schema
    )

    for data in email_list:
        # Trigger any available delivery report callbacks from previous produce() calls
        avroProducer.poll(0)

        message_id = str(uuid.uuid4())
        data["Request_id"] = message_id

        # Asynchronously produce a message, the delivery report callback will be triggered from poll() above, or flush() below, when the message has been successfully delivered or failed permanently.
        avroProducer.produce(topic="app-messages", value=data)
        print("\033[1;31;40m -- PRODUCER: Sent message with id {}\033[0;0m".format(message_id))

    avroProducer.flush()


def check_inbox():
    num_unread_msgs = count_unread_emails(["INBOX"])

    while num_unread_msgs != 0:
        messages = get_emails(["INBOX"])
        avro_producer(messages)
        num_unread_msgs = count_unread_emails(["INBOX"])


value_schema_str = """
{
    "namespace": "IntelligentEmailArchive",
    "name": "Email",
    "type": "record",
    "fields": [
    {"name": "From", "type": "string"},
    {"name": "To", "type": "string"},
    {"name": "Subject", "type": "string"},
    {"name": "Date_time", "type": "string"},
    {"name": "Message_body", "type": "string"},
    {"name": "Request_id", "type": "string", "logicalType": "uuid"}
    ]
}
"""


value_schema = avro.loads(value_schema_str)


messages = get_emails(["INBOX"])
avro_producer(messages)

schedule.every(2).minutes.do(check_inbox)

while True:
    schedule.run_pending()
    time.sleep(1)


if __name__ == "__main__":
    topics = ["app-messages", "retrain"]
    call_create_topic(client, topics)
    time.sleep(2)
