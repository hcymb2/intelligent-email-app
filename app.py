from __future__ import print_function

import base64
import os.path
import pprint

from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


def get_gmail_service():
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('./google-auth/token.json'):
        creds = Credentials.from_authorized_user_file('./google-auth/token.json', SCOPES)
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


def get_emails():

    service = get_gmail_service()

# request a list of all the messages
    result = service.users().messages().list(userId='me', maxResults=4).execute()
  
    messages = result.get('messages', [])
  
    # messages is a list of dictionaries where each dictionary contains a message id. iterate through all the messages
    for msg in messages:
        # Get the message from its id
        txt = service.users().messages().get(userId='me', id=msg['id']).execute()

        # Use try-except to avoid any Errors
        try:
            # Get value of 'payload' from dictionary 'txt'
            payload = txt['payload']
            headers = payload['headers']
  
            # Look for Subject and Sender Email in the headers
            for d in headers:
                if d['name'] == 'Subject':
                    subject = d['value']
                if d['name'] == 'From':
                    sender = d['value']
  
            # The Body of the message is in Encrypted format. So, we have to decode it.
            # Get the data and decode it with base 64 decoder.
            parts = payload.get('parts')[0]
            data = parts['body']['data']
            data = data.replace("-","+").replace("_","/") # decoding from Base64 to UTF-8
            decoded_data = base64.b64decode(bytes(data, 'UTF-8'))
  
            # Now, the data obtained is in lxml. So, we will parse 
            # it with BeautifulSoup library
            soup = BeautifulSoup(decoded_data , "lxml")
            body = soup.body()
  
            # Printing the subject, sender's email and message
            print("Subject: ", subject)
            print("From: ", sender)
            print("Message: ", body)
            print('\n')
        except Exception as e:
            print(e)
            # temp_dict = None
            pass


if __name__ == '__main__':
    get_emails()
    # for message in messages:
    #     print(message['id'])
    # pprint.pprint(get_email_content('178dc5703daccaf2'))
