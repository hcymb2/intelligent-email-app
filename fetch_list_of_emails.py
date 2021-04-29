## This code returns a list of email dictionaries.

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

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
user_id = 'me'


def get_gmail_service():
    # If modifying these scopes, delete the file token.json.

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('./google-auth/token.json'):
        creds = Credentials.from_authorized_user_file(
            './google-auth/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                './google-auth/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('./google-auth/token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service


def ReadEmailDetails(user_id):

    service = get_gmail_service()

    label_ids = GetLabelID(service, ["INBOX"]) #need to use the label ID not name.
    unread_msgs = service.users().messages().list(userId=user_id,  maxResults=2, labelIds=label_ids, q='label:unread').execute()
    #messages = unread_msgs.get('messages', [])

    messages = []

    if 'messages' in unread_msgs:
        messages.extend(unread_msgs['messages'])

    while 'nextPageToken' in unread_msgs:
        page_token = unread_msgs['nextPageToken']

        unread_msgs = service.users().messages().list(userId=user_id, labelIds=label_ids, pageToken=page_token, maxResults=4, q='label:unread').execute()
        #unread_msgs = service.users().messages().list(userId=user_id, pageToken=page_token, maxResults=3, q='from:thomas@collegeinfogeek.com label:unread').execute()

        messages.extend(unread_msgs['messages'])

        print('... total %d emails on next page [page token: %s], %d listed so far' % (
            len(unread_msgs['messages']), page_token, len(messages)))
        sys.stdout.flush()

    print ("Total unread messages in inbox: ", str(len(messages)))
    
    final_list = [ ]

    for msg in messages:
        email_dict = {}

        # Use try-except to avoid any Errors
        try:
            
            # Get value of 'payload' from dictionary 'txt'
            content = service.users().messages().get(userId=user_id, id=msg['id']).execute()  # fetch the message using API

            payload = content['payload']
            headers = payload['headers']

            # Look for Subject, Receiver Email, Sender Email, Date in the headers
            for look in headers:
                if look['name'] == 'Subject':
                    subject = look['value']
                    email_dict['Subject'] = subject

                if look['name'] == 'To':
                    receiver = look['value']
                    email_dict['Receiver'] = receiver

                if look['name'] == 'From':
                    sender = look['value']
                    email_dict['Sender'] = sender

                if look['name'] == 'Date':
                    msg_date = look['value']
                    date_parse = (parser.parse(msg_date))
                    m_date = (date_parse.date())
                    email_dict['DateTime'] = m_date

            # The Body of the message is in Encrypted format. So, we have to decode it.
            # Get the data and decode it with base 64 decoder.
            data = payload['body']['data']
            # decoding from Base64 to UTF-8
            data = data.replace("-", "+").replace("_", "/")
            decoded_data = base64.b64decode(bytes(data, 'UTF-8'))

            # Now, the data obtained is in lxml. So, we will parse
            # it with BeautifulSoup library
            soup = BeautifulSoup(decoded_data, "lxml")
            body = soup.body()
            email_dict['Message_body'] = body

            # Printing the subject, sender's email and message
            #pprint.pprint(email_dict)
            #print("Subject: ", subject)


        except Exception as e:
            print(e)
            email_dict = None
            pass


        # print(email_dict)
        final_list.append(email_dict) # This will create a dictonary item in the final list (list of email dictionaries)
        print(final_list)

        #mark the message as read
        #service.users().messages().modify(userId=user_id, id=msg['id'], body={ 'removeLabelIds': ['UNREAD']}).execute()

    print ("Total messaged retrived: ", str(len(final_list)))

def ListMessagesWithLabels(service):

    
    label_ids = GetLabelID(service, ["INBOX"]) #need to use the label ID not name.

    try:
        unread_msgs = service.users().messages().list(userId=user_id,  maxResults=4, labelIds=label_ids, q='label:unread').execute()
        #unread_msgs = service.users().messages().list(userId=user_id,  maxResults=3, q='from:thomas@collegeinfogeek.com label:unread').execute()

        # print(unread_msgs)
        
        messages = unread_msgs.get('messages', [])

        messages = []
        if 'messages' in unread_msgs:
            messages.extend(unread_msgs['messages'])

        while 'nextPageToken' in unread_msgs:
            page_token = unread_msgs['nextPageToken']

            unread_msgs = service.users().messages().list(userId=user_id, labelIds=label_ids, pageToken=page_token, maxResults=4, q='label:unread').execute()
            #unread_msgs = service.users().messages().list(userId=user_id, pageToken=page_token, maxResults=3, q='from:thomas@collegeinfogeek.com label:unread').execute()

            messages.extend(unread_msgs['messages'])

            print('... total %d emails on next page [page token: %s], %d listed so far' % (
                len(unread_msgs['messages']), page_token, len(messages)))
            sys.stdout.flush()

        print ("Total unread messages in inbox: ", str(len(messages)))
        return messages

    except HttpError as error:
        print('An error occurred: %s' % error)

    return {}
    

def GetLabelID(service, LabelName):
    # Conversion function - takes list of label names, returns corresponding label ID's
    results = service.users().labels().list(userId='me').execute()
    labels = results.get('labels', [])
    
    labelID = []
    for d in LabelName:
        for label in labels:
            if label['name'] == d:
                labelID.append(label['id'])

    return labelID

    # if not labels:
    #     print('No labels found.')
    # else:
    #     print('Labels:')
    #     for label in labels:
    #         print(label['name'] + " " + str(label['id']))


if __name__ == '__main__':
    
    GMAIL = get_gmail_service()

    save_email = ReadEmailDetails(user_id)

    #GetLabels(GMAIL)
    # labelIDs = GetLabelID(GMAIL, ["SEGP", "INBOX"])
    # print(labelIDs)
    
    
    # email_list = ListMessagesWithLabels(GMAIL)
    # print(email_list)


    # for email in email_list:
    #     #print(email['id'])
    #     # get id of individual message
    #     email_dict = ReadEmailDetails(user_id, str(email['id']))
