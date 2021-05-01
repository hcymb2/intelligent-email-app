from confluent_kafka.admin import AdminClient

from fetch_email_dict import count_unread_emails, get_emails
from kafka_clients import producer
from kafka_topics import call_create_topic


class Observer:
    _observers = []

    def __init__(self):
        self._observers.append(self)
        self._observed_events = []

    def observe(self, event_name, callback_fn):
        self._observed_events.append(
            {"event_name": event_name, "callback_fn": callback_fn}
        )


class Event:
    def __init__(self, event_name, *callback_args):
        for observer in Observer._observers:
            for observable in observer._observed_events:
                if observable["event_name"] == event_name:
                    observable["callback_fn"](*callback_args)


class Email(Observer):
    def __init__(self):
        print("Observing emails.")
        Observer.__init__(self)  # DON'T FORGET THIS

    def got_unread_emails(self, num_unread_msgs):
        print(f"Inbox has {num_unread_msgs} unread emails.")
        ### poll GMAIL repeatedly
        messages = get_emails(["INBOX"])
        producer(messages)


client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})

# Checks if kafka topics exist and creates the topic if it does not
topics = ["app-messages", "retrain"]
call_create_topic(client, topics)

num_unread_msgs = count_unread_emails(["INBOX"])
inbox_listener = Email()

# Observe for specific event - checking if email has unread messages
while 1:
    inbox_listener.observe(num_unread_msgs != 0, inbox_listener.got_unread_emails)
    Event(num_unread_msgs != 0, num_unread_msgs)



# # Observe for specific event
# email = Email()
# email.observe('someone arrived',  email.got_unread_email)

# # Fire some events

# Event('someone arrived', 'Lenard') # will output "Lenard has arrived!"
# Event('someone Farted',  'Lenard')
