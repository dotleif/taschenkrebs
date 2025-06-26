#!/usr/bin/env python3
"""
Runs on Strand-fe2: ssh 10.64.3.2 -l bockelma

Fetch unread Gmail messages with subject starting "Drifter Hereon",
download any CSV attachments, append them to taschenkrebs.csv in this script’s folder,
send email alerts if any buoy has moved > 50m from its home position,
and move processed messages into the Gmail label "Drifter_Hereon".
"""

import os
import io
import math
import base64   
import pandas as pd
from datetime import datetime
from email.mime.text import MIMEText
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
SCOPES           = ['https://www.googleapis.com/auth/gmail.modify']
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
PY_SCRIPT        = 'taschenkrebs.py'
CSV_FILE         = 'drifters_hereon.csv'
MAP_HTML         = 'drifters_hereon_map.html'
PROCESSED_LABEL  = 'Drifter_Hereon'
REPO             = 'taschenkrebs'
TOKEN_FILE       = os.path.join(BASE_DIR, 'token.json')
MASTER_CSV       = os.path.join(BASE_DIR, CSV_FILE)
ALERT_THRESHOLD  = 50.0  # meters
NOTIFY_EMAIL     = 'frank-detlef.bockelmann@hereon.de'  # adjust as needed
ALERT_LOG_FILE   = os.path.join(BASE_DIR, 'alerted.json')
# ──────────────────────────────────────────────────────────────────────────────

import json
import folium
import subprocess

def log(msg: str):
    """Print a timestamped message."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}")

def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance (in meters) between two lat/lon points."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def create_message(sender, to, subject, body):
    msg = MIMEText(body)
    msg['to'] = to
    msg['from'] = sender
    msg['subject'] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return {'raw': raw}

def send_notification(service, sender, recipient, subject, body):
    """Send an email via Gmail API."""
    message = create_message(sender, recipient, subject, body)
    service.users().messages().send(userId='me', body=message).execute()

def get_service():
    """Load credentials, refresh if needed, and build Gmail service."""
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError(f"{TOKEN_FILE} missing or invalid; run authorize.py first")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)

def ensure_label(service):
    """Get or create the Gmail label for processed messages."""
    labels = service.users().labels().list(userId='me').execute().get('labels', [])
    for L in labels:
        if L['name'] == PROCESSED_LABEL:
            return L['id']
    body = {
        'name': PROCESSED_LABEL,
        'labelListVisibility': 'labelShow',
        'messageListVisibility': 'show'
    }
    new_label = service.users().labels().create(userId='me', body=body).execute()
    return new_label['id']

def generate_map():
    # 1) Read the master CSV and pull out the latest per buoy
    df = pd.read_csv(
        MASTER_CSV,
        parse_dates=['date_UTC'],
        date_format='%d-%b-%Y %H:%M:%S',
        dtype={'D_number': str},
        skipinitialspace=True,
        encoding='utf-8-sig'
    )
    df['D_number'] = df['D_number'].str.strip()
    latest = (
        df.sort_values('date_UTC')
          .groupby('D_number', as_index=False)
          .last()
    )

    # 2) Build a Folium map centered on the mean location
    center = [ latest['Latitude'].mean(), latest['Longitude'].mean() ]
    m = folium.Map(location=center, zoom_start=6)

    # 3) Add one marker per buoy
    for _, row in latest.iterrows():
        folium.Marker(
            [row['Latitude'], row['Longitude']],
            popup=f"<b>{row['D_number']}</b><br>{row['date_UTC']}"
        ).add_to(m)

    # 4) Save to HTML in your script folder
    out = os.path.join(BASE_DIR, MAP_HTML)
    m.save(out)
    #log(f"Map updated: {out}")

def fetch_and_append():
    service  = get_service()
    label_id = ensure_label(service)


    # Load or init the set of buoys we’ve already alerted on
    if os.path.exists(ALERT_LOG_FILE):
        with open(ALERT_LOG_FILE) as f:
            alerted = json.load(f)
    else:
        alerted = {}

    # Fetch all unread messages IDs with subject prefix
    query = 'is:unread subject:"Drifters Hereon"'
    resp  = service.users().messages().list(userId='me', q=query).execute()
    items = resp.get('messages', [])

    if not items:
        print("No new messages.")
        return
    
    # 2) For each message ID, fetch its internalDate and collect
    dated = []
    for it in items:
        meta = service.users().messages().get(
            userId='me',
            id=it['id'],
            format='minimal'   # still returns internalDate
        ).execute()
        dated.append({
            'id':           it['id'],
            'internalDate': int(meta['internalDate'])
        })

    # 3) Sort oldest → newest
    dated.sort(key=lambda x: x['internalDate'])

    any_processed = False

    for entry in dated:
        msg_id = entry['id']
        msg = service.users().messages().get(
            userId='me', id=msg_id, format='full').execute()

        parts = msg.get('payload', {}).get('parts', [])
        processed = False

        for part in parts:
            fname = part.get('filename', '')
            body  = part.get('body', {})
            if fname.lower().endswith('.csv') and 'attachmentId' in body:
                # Download and parse attachment
                att = service.users().messages().attachments().get(
                    userId='me', messageId=msg_id, id=body['attachmentId']
                ).execute()
                raw_data = base64.urlsafe_b64decode(att['data'])
                df = pd.read_csv(
                    io.BytesIO(raw_data), 
                    parse_dates=['date_UTC'],
                    date_format='%d-%b-%Y %H:%M:%S',    # e.g. 17-Jun-2025 14:31:39
                    dtype={'D_number': str},            # ← force string
                    skipinitialspace=True,              # in case of stray spaces
                    encoding='utf-8-sig'
                )
                df['D_number'] = df['D_number'].str.strip()  # clean whitespace

                # Compute alerts if master exists
                if os.path.exists(MASTER_CSV):
                    hist = pd.read_csv(
                        MASTER_CSV, 
                        parse_dates=['date_UTC'],
                        dtype={'D_number': str},        # force string here too
                        skipinitialspace=True,
                        encoding='utf-8-sig'
                    )
                    hist['D_number'] = hist['D_number'].str.strip()
                    home = (
                        hist.sort_values('date_UTC')
                            .groupby('D_number', as_index=False)
                            .first()[['D_number','Latitude','Longitude']]
                            .rename(columns={'Latitude':'lat_home','Longitude':'lon_home'})
                    )
                    current = (
                        df.sort_values('date_UTC')
                          .groupby('D_number', as_index=False)
                          .last()[['D_number','Latitude','Longitude']]
                          .rename(columns={'Latitude':'lat_current','Longitude':'lon_current'})
                    )
                    merged = pd.merge(current, home, on='D_number')
                    for _, row in merged.iterrows():
                        dist = haversine(
                            row.lat_home, row.lon_home,
                            row.lat_current, row.lon_current
                        )
                        # Only alert once per buoy ever
                        if dist > ALERT_THRESHOLD and row.D_number not in alerted:
                            subj = f"Alert: Buoy {row.D_number} moved {dist:.1f} m"
                            body = (
                                f"Buoy ID: {row.D_number}\n"
                                f"Distance moved: {dist:.1f} meters\n"
                                f"Home pos: ({row.lat_home:.5f}, {row.lon_home:.5f})\n"
                                f"Current: ({row.lat_current:.5f}, {row.lon_current:.5f})"
                            )
                            send_notification(service, NOTIFY_EMAIL, NOTIFY_EMAIL, subj, body)
                            #print(f"Sent alert for {row.D_number}: {dist:.1f} m")
                            log(f"Sent alert for {row.D_number}: {dist:.1f} m")
                            alerted[row.D_number] = datetime.now().isoformat()
                # After processing all messages, persist the updated alert-log
                with open(ALERT_LOG_FILE, 'w') as f:
                    json.dump(alerted, f, indent=2)
                
                # Append to master CSV
                write_hdr = not os.path.exists(MASTER_CSV) or os.path.getsize(MASTER_CSV) == 0
                df.to_csv(MASTER_CSV, mode='a', header=write_hdr, index=False)
                #print(f"Appended {len(df)} rows from {fname}")
                log(f"Appended {len(df)} rows from {fname}")
                processed = True

        if processed:
            # mark as read and label
            service.users().messages().modify(
                userId='me', id=msg_id,
                body={'removeLabelIds':['UNREAD']}
            ).execute()
            service.users().messages().modify(
                userId='me', id=msg_id,
                body={'addLabelIds':[label_id]}
            ).execute()
            #print(f"Processed and labeled message {msg_id}")
            any_processed = True

    if any_processed:
        # 1) Update MAP_HTML
        generate_map()
        
        # 2) copy into your GitHub Pages repo
        repo_dir = os.path.join(BASE_DIR, REPO)
        # HTML -> docs/index.html
        src_html = os.path.join(BASE_DIR, MAP_HTML)
        dst_html = os.path.join(repo_dir, 'docs', 'index.html')
        # CSV -> root of repo
        src_csv = MASTER_CSV
        dst_csv = os.path.join(repo_dir, CSV_FILE)
        # .py script
        src_py = PY_SCRIPT
        dst_py = os.path.join(repo_dir, PY_SCRIPT)
        for src, dst in ((src_html, dst_html), 
                         (src_csv, dst_csv),
                         (src_py, dst_py)):
            subprocess.run(['cp', src, dst], check=True)

        # 3) commit & push
        commit_msg = f"Auto-update {datetime.now():%Y-%m-%d %H:%M:%S'}"
        cmds = [
            ['git', 'add', 'docs/index.html', CSV_FILE, PY_SCRIPT],
            ['git', 'commit', '-m', commit_msg],
            ['git', 'push', 'origin', 'main'],
        ]
        for cmd in cmds:
            subprocess.run(cmd, cwd=repo_dir, check=True)

if __name__ == '__main__':
    try:
        fetch_and_append()
    except HttpError as e:
        #print(f"API error: {e}")
        log(f"API error: {e}")
        exit(1)
    except Exception as e:
        #print(f"Error: {e}")
        log(f"Error: {e}")
        exit(1)
