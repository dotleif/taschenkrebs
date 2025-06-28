#!/usr/bin/env python3
# authorize.py
import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES     = ['https://www.googleapis.com/auth/gmail.modify',
               'https://www.googleapis.com/auth/gmail.send']

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CRED_FILE  = os.path.join(BASE_DIR, 'credentials.json')
TOKEN_FILE = os.path.join(BASE_DIR, 'token.json')

def main():
    flow = InstalledAppFlow.from_client_secrets_file(CRED_FILE, SCOPES)
    # opens browser and listens on a localhost port for the redirect
    creds = flow.run_local_server(port=0, prompt='consent')

    with open(TOKEN_FILE, 'w') as f:
         f.write(creds.to_json())
    print(f"Authorized! {TOKEN_FILE} written")

if __name__ == '__main__':
    main()
