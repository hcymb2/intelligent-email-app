from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import uuid
from time import sleep

BROKER_URL = "PLAINTEXT://localhost:9092"


def avro_producer(messages):

    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    value_schema_str = """
    {
      "namespace": "IntelligentEmailArchive",
      "name": "Email",
      "type": "record",
      "fields": [
        {"name": "Request_id", "type": "string", "logicalType": "uuid"},
        {"name": "DateTime", "type": "int", "logicalType": "date"},
        {"name": "From", "type": "string"},
        {"name": "To", "type": "string"},
        {"name": "Subject", "type": "string"},
        {"name": "Message_body", "type": "string"}
      ]
    }
    """

    value_schema = avro.loads(value_schema_str)

    avroProducer = AvroProducer(
        {
            "bootstrap.servers": BROKER_URL,
            "on_delivery": delivery_report,
            "schema.registry.url": "http://schema-registry:8081",
        },
        default_value_schema=value_schema,
    )

    for data in messages:
        # Trigger any available delivery report callbacks from previous produce() calls
        avroProducer.poll(0)

        message_id = str(uuid.uuid4())
        message = {
            "Request_id": message_id,
            "DateTime": data["DateTime"],
            "From": data["From"],
            "To": data["To"],
            "Subject": data["Subject"],
            "Message_body": data["Message_body"],
        }

        # Asynchronously produce a message, the delivery report callback will be triggered from poll() above, or flush() below, when the message has been successfully delivered or failed permanently.
        avroProducer.produce(topic="app-messages", value=message)
        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        sleep(1)

    avroProducer.flush()
