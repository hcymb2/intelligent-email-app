from confluent_kafka.admin import AdminClient, NewTopic


def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    topic_metadata = client.list_topics(timeout=5)
    return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""

    # Call create_topics to asynchronously create topics, a dict of <topic,future> is returned.
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=1,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                },
            )
        ]
    )

    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0 to the create_topics() call.
    # All futures will finish at the same time.
    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")


def delete_topics(a, topics):
    """delete topics

     Parameters:
    ----------
    a : AdminClient
    topics : List of topics e.g. ["topic1", "topic2"]

    """

    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while topics are deleted in the background.
    # But here we give it some time (30s) to propagate in the cluster before returning.

    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))
