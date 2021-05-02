## This code returns each email dictionary.

from __future__ import print_function

import base64
import os.path
import pprint
import sys
import time
from datetime import datetime

import dateutil.parser as parser
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import Http

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
user_id = "me"


def get_gmail_service():
    # If modifying these scopes, delete the file token.json.

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists("./google-auth/token.json"):
        creds = Credentials.from_authorized_user_file(
            "./google-auth/token.json", SCOPES
        )
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "./google-auth/credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("./google-auth/token.json", "w") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)
    return service


service = get_gmail_service()


def ReadEmailDetails(user_id, msg_id):

    email_dict = {}

    # Use try-except to avoid any Errors
    try:

        # Get value of 'payload' from dictionary 'txt'
        content = (
            service.users().messages().get(userId=user_id, id=msg_id).execute()
        )  # fetch the message using API

        payload = content["payload"]
        headers = payload["headers"]

        # Look for Subject, Receiver Email, Sender Email, Date in the headers
        for look in headers:
            if look["name"] == "Subject":
                subject = look["value"]
                email_dict["Subject"] = subject

            if look["name"] == "To":
                receiver = look["value"]
                email_dict["To"] = receiver

            if look["name"] == "From":
                sender = look["value"]
                email_dict["From"] = sender

            if look["name"] == "Date":
                msg_date = look["value"]
                date_parse = parser.parse(msg_date)
                m_date = date_parse.strftime("%Y/%m/%d %H:%M:%S %z %Z")
                email_dict["Date_time"] = m_date

        # The Body of the message is in Encrypted format. So, we have to decode it.
        # Get the data and decode it with base 64 decoder.
        data = payload["body"]["data"]
        # decoding from Base64 to UTF-8
        data = data.replace("-", "+").replace("_", "/")
        decoded_data = base64.b64decode(bytes(data, "UTF-8"))

        # Now, the data obtained is in lxml. So, we will parse it with BeautifulSoup library
        soup = BeautifulSoup(decoded_data, "lxml")
        body = soup.body()
        email_dict["Message_body"] = body

    except Exception as e:
        print(f"Exception occured {e}")
        email_dict = None

    finally:
        # mark the message as read
        service.users().messages().modify(
            userId=user_id, id=msg_id, body={"removeLabelIds": ["UNREAD"]}
        ).execute()

        return email_dict
        #pprint.pprint(email_dict)

        


def ListMessagesWithLabels(labels):

    label_ids = GetLabelID(labels)  # need to use the label ID not name.

    try:
        unread_msgs = (
            service.users()
            .messages()
            .list(userId=user_id, maxResults=10, labelIds=label_ids, q="label:unread")
            .execute()
        )

        # print(unread_msgs)

        messages = unread_msgs.get("messages", [])

        print("Total unread messages in inbox: ", str(len(messages)))
        return messages

    except HttpError as error:
        print("An error occurred: %s" % error)


def GetLabelID(LabelName):
    # Conversion function - takes list of label names, returns corresponding label ID's
    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])

    labelID = []
    for d in LabelName:
        for label in labels:
            if label["name"] == d:
                labelID.append(label["id"])

    return labelID

    # if not labels:
    #     print('No labels found.')
    # else:
    #     print('Labels:')
    #     for label in labels:
    #         print(label['name'] + " " + str(label['id']))


def get_emails(labels):
    """Gets emails ID's and reads the email content and batches all unread email in one run in a list"""

    email_list = ListMessagesWithLabels(labels)

    final_list = []

    for email in email_list:
        # get content of individual message from its id
        email_dict = ReadEmailDetails(user_id, str(email["id"]))

        if email_dict is None:
            continue

        final_list.append(email_dict)

    print(f"Total unread messages retrieved: {len(final_list)}")
    pprint.pprint(final_list)
    return final_list


def count_unread_emails(labels):
    unread_messages = ListMessagesWithLabels(labels)
    return len(unread_messages)


if __name__ == "__main__":
    pass
    # GetLabels(GMAIL)
    # labelIDs = GetLabelID(["SEGP", "INBOX"])
    # print(labelIDs)
