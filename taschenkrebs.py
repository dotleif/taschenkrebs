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
import json
import folium
import subprocess
from datetime import datetime
from folium import Html
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
CSV_FILE         = 'drifters_hereon.csv'
MAP_HTML         = 'drifters_hereon_map.html'
PROCESSED_LABEL  = 'Drifter_Hereon'
REPO             = 'taschenkrebs'
TOKEN_FILE       = os.path.join(BASE_DIR, 'token.json')
MASTER_CSV       = os.path.join(BASE_DIR, CSV_FILE)
HOME_CSV         = os.path.join(BASE_DIR, 'home_positions.csv')
ALERT_THRESHOLD  = 50.0  # meters
NOTIFY_EMAIL     = 'frank-detlef.bockelmann@hereon.de'  # adjust as needed
ALERT_LOG_FILE   = os.path.join(BASE_DIR, 'alerted.json')
# ──────────────────────────────────────────────────────────────────────────────



def log(msg: str):
    """Print a timestamped message."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}")

def load_home_positions():
    # HOME_CSV must be provided; we won’t auto-generate it.
    if not os.path.exists(HOME_CSV):
        raise RuntimeError(f"{HOME_CSV} not found.")
    # Load the provided home positions file
    home = pd.read_csv(
        HOME_CSV,
        dtype={'D_number': str},
        skipinitialspace=True,
        encoding='utf-8-sig'
    )
    # rename columns: keep D_number
    # map Latitude/Longitude to lat_home/lon_home, last col date_UTC
    home = home.rename(columns={
        'Latitude': 'lat_home',
        'Longitude': 'lon_home',
        home.columns[-1]: 'date_UTC'
    })
    # parse the activation timestamp
    home['date_UTC'] = pd.to_datetime(
        home['date_UTC'],
        format='%Y-%m-%d %H:%M:%S'
    )
    # strip whitespace from D_number
    home['D_number'] = home['D_number'].str.strip()
    # set D_number as index and keep only the three needed columns
    home = home.set_index('D_number')[['lat_home','lon_home','date_UTC']]
    return home

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
    # 1) Load home positions
    home_df = load_home_positions().reset_index()
    # 2) Try to load the pre-computed latest positions
    lp = os.path.join(BASE_DIR, 'latest_positions.csv')
    if os.path.exists(lp):
        df = pd.read_csv(
            lp,
            parse_dates=['date_UTC'] #,
            #date_format='%d-%b-%Y %H:%M:%S'
        )
        df['D_number'] = df['D_number'].astype(str).str.strip()
        latest = df
    else:
        # fallback: read full MASTER_CSV and compute latest
        df = pd.read_csv(
            MASTER_CSV,
            parse_dates=['date_UTC'],
            #date_format='%d-%b-%Y %H:%M:%S',
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
    # 3) Build a Folium map centered on the mean of home + current
    all_lats = list(home_df['lat_home']) + list(latest['Latitude'])
    all_lons = list(home_df['lon_home']) + list(latest['Longitude'])
    center = [sum(all_lats) / len(all_lats), sum(all_lons) / len(all_lons)]
    m = folium.Map(location=center, zoom_start=10)
    # 4) Plot current positions
    for _, row in latest.iterrows():
        battery = row.get('batteryState', '').strip().upper()
        if battery == 'GOOD':
            state = 'good'
            col = 'green'
        elif battery == 'LOW':
            state = 'low'
            col = 'orange'
        else:
            state = 'unknown'
            col = 'red'
        # build HTML popup
        popup_html = (
            f"<b>{row['D_number']}</b><br>"
            f"Status  : {state}<br>"
            f"DateTime: {row['date_UTC']}<br>"
            f"Distance: {row['distance_m']:.1f} m"
        )
        # render popup_html as real HTML
        popup = folium.Popup(
            Html(popup_html, script=True),
            max_width=250
        )
        # use a Font-Awesome buoy-like icon (here 'info-sign') for latest
        icon = folium.Icon(
            icon='info-sign',
            prefix='glyphicon',
            color=col
        )
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=popup,
            icon=icon
        ).add_to(m)   
    # 5) Plot home positions in black
    for _, row in home_df.iterrows():
        folium.CircleMarker(
            location=[row.lat_home, row.lon_home],
            radius=6,
            color='black',
            fill=True,
            fill_color='black',
            fill_opacity=0.5,
            popup=f"{row.D_number} (home)"
        ).add_to(m)
    # 6) Save to HTML in your script folder
    out = os.path.join(BASE_DIR, MAP_HTML)
    m.save(out)
    #log(f"Map updated: {out}")

def fetch_and_append():
    service  = get_service()
    label_id = ensure_label(service)
    seen_ids = set()
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
                    #date_format='%d-%b-%Y %H:%M:%S',    # e.g. 17-Jun-2025 14:31:39
                    dtype={'D_number': str},            # ← force string
                    skipinitialspace=True,              # in case of stray spaces
                    encoding='utf-8-sig'
                )
                df['D_number'] = df['D_number'].str.strip()  # clean whitespace
                # remember these buoys as “seen” this batch
                seen_ids.update(df['D_number'].tolist())
                # filter out any records before the buoy’s activation
                home_df = load_home_positions()
                # build a temporary Series of activation times
                activation_times = df['D_number'].map(home_df['date_UTC'])
                # only keep rows where date_UTC > activation, without adding any column
                df = df.loc[df['date_UTC'] > activation_times].copy()

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

                    home = load_home_positions()
                    current = (
                        df.sort_values('date_UTC')
                          .groupby('D_number', as_index=False)
                          .last()[['D_number','Latitude','Longitude']]
                          .rename(columns={'Latitude':'lat_current','Longitude':'lon_current'})
                          .set_index('D_number')
                    )
                    merged = home.join(
                        current[['lat_current','lon_current']],how='inner'
                    )
                    for buoy, row in merged.iterrows():
                        dist = haversine(
                            row.lat_home, row.lon_home,
                            row.lat_current, row.lon_current
                        )
                        # Only alert once per buoy ever
                        if dist > ALERT_THRESHOLD and buoy not in alerted:
                            subj = f"Alert: Buoy {buoy} moved {dist:.1f} m"
                            body = (
                                f"Buoy ID: {buoy}\n"
                                f"Distance moved: {dist:.1f} meters\n"
                                f"Home pos: ({row.lat_home:.5f}, {row.lon_home:.5f})\n"
                                f"Current: ({row.lat_current:.5f}, {row.lon_current:.5f})"
                            )
                            send_notification(service, NOTIFY_EMAIL, NOTIFY_EMAIL, subj, body)
                            log(f"Sent alert for {buoy}: {dist:.1f} m")
                            alerted[buoy] = datetime.now().isoformat()
                    # Missing transmission alert when any home position is not in this 
                    # batch's current positions.
                    # Identify this batch by its internalDate and subject and
                    # embeds both into the alert subject and body.
                    batch_time = datetime.fromtimestamp(entry['internalDate']/1000.0)
                    headers    = msg.get('payload', {}).get('headers', [])
                    subject_line = next(
                        (h['value'] for h in headers if h['name']=='Subject'),
                        '<no-subject>'
                    )
                    current_ids = set(current.index)
                    home_ids    = set(home.index)
                    for buoy in home_ids - current_ids:
                        if buoy not in alerted:
                            subj = (
                                f"Alert: Buoy {buoy} missing "
                                f"in batch {batch_time:%Y-%m-%d %H:%M:%S}"
                            )
                            body = (
                                f"Buoy {buoy} did not transmit in this batch.\n\n"
                                f"Email Subject: {subject_line}\n"
                                f"Batch Received: {batch_time:%Y-%m-%d %H:%M:%S}\n\n"
                                f"Check attached CSV from the email for details."
                            )
                            send_notification(
                                service, NOTIFY_EMAIL, NOTIFY_EMAIL, subj, body
                            )
                            alerted[buoy] = datetime.now().isoformat()

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
        # 2) also write out the small latest_positions.csv, tagging missing buoys
        #   a) load full history and compute last‐known per buoy
        master = pd.read_csv(
            MASTER_CSV,
            parse_dates=['date_UTC'],
            dtype={'D_number': str},
            skipinitialspace=True,
            encoding='utf-8-sig'
        )
        latest = (
            master.sort_values('date_UTC')
                  .groupby('D_number', as_index=False)
                  .last()[['D_number','Latitude','Longitude','date_UTC','batteryState']]
        )
        home = load_home_positions().reset_index()[['D_number','lat_home','lon_home']]
        latest = latest.merge(home, on='D_number', how='left')
        latest['distance_m'] = latest.apply(
            lambda r: haversine(r.lat_home, r.lon_home, r.Latitude, r.Longitude),
            axis=1
        )
        # b) for any home buoy not in seen_ids, override its state
        home = load_home_positions()
        missing = set(home.index) - seen_ids
        for buoy in missing:
            latest.loc[latest['D_number']==buoy, 'batteryState'] = 'UNKNOWN'
        #  c) write out
        latest.to_csv(os.path.join(BASE_DIR, 'latest_positions.csv'), index=False)
        # 2) copy into your GitHub Pages repo
        repo_dir = os.path.join(BASE_DIR, REPO)
        # HTML -> docs/index.html
        src_html = os.path.join(BASE_DIR, MAP_HTML)
        dst_html = os.path.join(repo_dir, 'docs', 'index.html')
        # CSV -> root of repo
        src_csv = MASTER_CSV
        dst_csv = os.path.join(repo_dir, CSV_FILE)
        for src, dst in ((src_html, dst_html), 
                         (src_csv, dst_csv)):
            subprocess.run(['cp', src, dst], check=True)

        # 3) commit & push
        commit_msg = f"Auto-update {datetime.now():%Y-%m-%d %H:%M:%S}"
        cmds = [
            ['git', 'add', 'docs/index.html', CSV_FILE],
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
