import asyncio
import json
import pprint
from datetime import date, datetime, timedelta

import avro
import pandas as pd
from confluent_kafka import Consumer, avro
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient

pd.set_option('display.max_columns', None)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"



async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    #
    #     Create a CachedSchemaRegistryClient
    #
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    #
    #     Use the Avro Consumer
    #
    c = AvroConsumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "0"},
        schema_registry=schema_registry,
    )

    ##So we create the an empty dataframe from before and as an email comes in we just append it to the dataframe
    column_names = ["bcc", "cc", "from",'receivedDate','replyTo','sender','sentDate','subject','textBody','to']

    df = pd.DataFrame(columns = column_names)

    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:

                pprint.pprint(message.value())
                #df=df.append(dictionary_parser(message.value()), ignore_index=True)
                #print(df.head())

            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)



def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("email"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(consume(topic_name))
    await t1


def dictionary_parser(message_value):
    
    ##Deleting contentType and htmlBody
    del message_value['contentType']
    del message_value['htmlBody']
    
    
    for key, value in message_value.items():
        ##converting these motherfuckers to strings if they are a list
        if isinstance(value, list):
            message_value[key] = ",".join(str(x) for x in value)
        if isinstance(value, datetime):
            message_value[key]= value.replace(tzinfo=None)
            message_value[key]= value.strftime("%H:%M:%S.%f - %b %d %Y")
        
    return message_value


if __name__ == "__main__":
    main()
