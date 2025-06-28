#!/usr/bin/env python3
"""
Runs on Strand-fe2: ssh 10.64.3.2 -l bockelma

Fetch unread Gmail messages with subject starting "Drifter Hereon",
download any CSV attachments, append them to drifters_hereon.csv in this script’s folder,
send email alerts if any buoy has moved > 50m from its home position or has gone silent,
and move processed messages into the Gmail label "Drifter_Hereon".
"""

import os
import io
import math
import base64
import json
import subprocess
from datetime import datetime

import pandas as pd
import folium
from folium import Html
from email.mime.text import MIMEText
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
SCOPES          = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.send',
]
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
CSV_FILE        = 'drifters_hereon.csv'
MAP_HTML        = 'drifters_hereon_map.html'
LATEST_CSV      = 'latest_positions.csv'
PROCESSED_LABEL = 'Drifter_Hereon'
TOKEN_FILE      = os.path.join(BASE_DIR, 'token.json')
MASTER_CSV      = os.path.join(BASE_DIR, CSV_FILE)
HOME_CSV        = os.path.join(BASE_DIR, 'home_positions.csv')
LATEST_CSV_PATH = os.path.join(BASE_DIR, LATEST_CSV)
ALERT_THRESHOLD = 50.0  # meters
NOTIFY_EMAIL    = 'frank-detlef.bockelmann@hereon.de'
ALERT_LOG_FILE  = os.path.join(BASE_DIR, 'alerted.json')
# ──────────────────────────────────────────────────────────────────────────────


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


def ensure_distance(df: pd.DataFrame, home_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge in home positions and compute `distance_m` if not already present.
    Expects home_df indexed by D_number with lat_home/lon_home columns.
    """
    if 'distance_m' not in df.columns:
        merge_df = home_df.reset_index()[['D_number', 'lat_home', 'lon_home']]
        df = df.merge(merge_df, on='D_number', how='left')
        df['distance_m'] = df.apply(
            lambda r: haversine(r.lat_home, r.lon_home, r.Latitude, r.Longitude),
            axis=1
        )
    return df


def load_home_positions() -> pd.DataFrame:
    """
    Load HOME_CSV, parse activation timestamp, and return a DataFrame
    indexed by D_number with ['lat_home','lon_home','date_UTC'] columns.
    """
    if not os.path.exists(HOME_CSV):
        raise RuntimeError(f"{HOME_CSV} not found.")
    home = pd.read_csv(
        HOME_CSV,
        dtype={'D_number': str},
        skipinitialspace=True,
        encoding='utf-8-sig'
    )
    home = home.rename(columns={
        'Latitude': 'lat_home',
        'Longitude': 'lon_home',
        home.columns[-1]: 'date_UTC'
    })
    home['date_UTC'] = pd.to_datetime(
        home['date_UTC'],
        format='%Y-%m-%d %H:%M:%S'
    )
    home['D_number'] = home['D_number'].str.strip()
    return home.set_index('D_number')[['lat_home','lon_home','date_UTC']]


def create_message(sender: str, to: str, subject: str, body: str) -> dict:
    msg = MIMEText(body)
    msg['to'] = to
    msg['from'] = sender
    msg['subject'] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return {'raw': raw}


def send_notification(service, sender: str, recipient: str, subject: str, body: str):
    """Send an email via Gmail API, logging any errors."""
    message = create_message(sender, recipient, subject, body)
    try:
        service.users().messages().send(userId='me', body=message).execute()
    except Exception as e:
        log(f"[ERROR] sending notification: {e}")


def get_service():
    """Load credentials, refresh if needed, and build Gmail service."""
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError(f"{TOKEN_FILE} missing or invalid; run authorize.py first")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)


def ensure_label(service) -> str:
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


def generate_map(home_df: pd.DataFrame):
    """
    Build the folium map:
     - Read or compute latest_positions.csv
     - Ensure distance_m is present
     - Render home (black) + latest (colored by batteryState/unknown) markers
    """
    lp = os.path.join(BASE_DIR, 'latest_positions.csv')
    if os.path.exists(lp):
        latest = pd.read_csv(lp, parse_dates=['date_UTC'])
    else:
        df_all = pd.read_csv(
            MASTER_CSV,
            parse_dates=['date_UTC'],
            dtype={'D_number': str},
            skipinitialspace=True,
            encoding='utf-8-sig'
        )
        df_all['D_number'] = df_all['D_number'].str.strip()
        latest = (
            df_all.sort_values('date_UTC')
                  .groupby('D_number', as_index=False)
                  .last()
        )
        if 'distance_m' not in latest.columns:
            home_merge = home_df.reset_index()[['D_number','lat_home','lon_home']]
            latest = latest.merge(home_merge, on='D_number', how='left')
            # make sure to apply on the *latest* slice, not the full df_all
            latest['distance_m'] = latest.apply(
                lambda r: haversine(r.lat_home, r.lon_home, r.Latitude, r.Longitude),
                axis=1
            )
    latest['D_number'] = latest['D_number'].astype(str).str.strip()
    latest = ensure_distance(latest, home_df)

    # center map on combined extents
    all_lats = list(home_df['lat_home']) + list(latest['Latitude'])
    all_lons = list(home_df['lon_home']) + list(latest['Longitude'])
    center = [sum(all_lats)/len(all_lats), sum(all_lons)/len(all_lons)]
    m = folium.Map(location=center, zoom_start=10)

    # plot latest
    for _, row in latest.iterrows():
        batt = row.get('batteryState', '').strip().upper()
        if batt == 'GOOD':
            col = 'green'
        elif batt == 'LOW':
            col = 'orange'
        else:
            col = 'red'
        popup_html = (
            f"<b>{row['D_number']}</b><br>"
            f"Status  : {batt or 'UNKNOWN'}<br>"
            f"DateTime: {row['date_UTC']}<br>"
            f"Distance: {row['distance_m']:.1f} m"
        )
        popup = folium.Popup(Html(popup_html, script=True), max_width=250)
        icon = folium.Icon(prefix='glyphicon', icon='info-sign', color=col)
        folium.Marker(
            [row['Latitude'], row['Longitude']],
            icon=icon,
            popup=popup
        ).add_to(m)

    # plot home
    for buoy, row in home_df.iterrows():
        folium.CircleMarker(
            [row.lat_home, row.lon_home],
            radius=6,
            color='black',
            fill=True, fill_color='black', fill_opacity=0.5,
            popup=f"{buoy} (home)"
        ).add_to(m)

    m.save(os.path.join(BASE_DIR, MAP_HTML))
    log("Map updated.")


def fetch_and_append():
    service  = get_service()
    label_id = ensure_label(service)
    home_df  = load_home_positions()

    # load alerted-log
    alerted = {}
    if os.path.exists(ALERT_LOG_FILE):
        with open(ALERT_LOG_FILE) as f:
            alerted = json.load(f)

    # fetch unread
    query = 'is:unread subject:"Drifters Hereon"'
    resp  = service.users().messages().list(userId='me', q=query).execute()
    items = resp.get('messages', [])
    if not items:
        log("No new messages.")
        return

    # sort oldest→newest
    dated = []
    for it in items:
        m = service.users().messages().get(userId='me', id=it['id'], format='minimal').execute()
        dated.append({'id': it['id'], 'ts': int(m['internalDate'])})
    dated.sort(key=lambda x: x['ts'])

    seen_ids = set()
    any_processed = False

    for entry in dated:
        msg = service.users().messages().get(userId='me', id=entry['id'], format='full').execute()
        parts = msg.get('payload', {}).get('parts', [])
        processed = False

        for p in parts:
            fname = p.get('filename', '')
            body  = p.get('body', {})
            if fname.lower().endswith('.csv') and 'attachmentId' in body:
                att = service.users().messages().attachments().get(
                    userId='me', messageId=entry['id'], id=body['attachmentId']
                ).execute()
                raw = base64.urlsafe_b64decode(att['data'])
                df = pd.read_csv(io.BytesIO(raw), parse_dates=['date_UTC'],
                                 dtype={'D_number': str}, skipinitialspace=True, encoding='utf-8-sig')
                df['D_number'] = df['D_number'].str.strip()
                seen_ids.update(df['D_number'])

                # filter by activation time
                act = df['D_number'].map(home_df['date_UTC'])
                df = df[df['date_UTC'] > act].copy()

                # alerts: movement + missing
                if os.path.exists(MASTER_CSV):
                    hist = pd.read_csv(MASTER_CSV, parse_dates=['date_UTC'],
                                       dtype={'D_number': str}, skipinitialspace=True, encoding='utf-8-sig')
                    hist['D_number'] = hist['D_number'].str.strip()
                    current = (
                        df.sort_values('date_UTC')
                          .groupby('D_number', as_index=False)
                          .last()[['D_number','Latitude','Longitude']]
                          .rename(columns={'Latitude':'lat_current','Longitude':'lon_current'})
                          .set_index('D_number')
                    )
                    merged = home_df.join(current, how='inner')
                    for buoy, row in merged.iterrows():
                        dist = haversine(row.lat_home, row.lon_home, row.lat_current, row.lon_current)
                        if dist > ALERT_THRESHOLD and buoy not in alerted:
                            subj = f"Alert: Buoy {buoy} moved {dist:.1f} m"
                            body = (
                                f"Buoy ID: {buoy}\n"
                                f"Distance moved: {dist:.1f} m\n"
                                f"Home pos: ({row.lat_home:.5f},{row.lon_home:.5f})\n"
                                f"Current: ({row.lat_current:.5f},{row.lon_current:.5f})"
                            )
                            send_notification(service, NOTIFY_EMAIL, NOTIFY_EMAIL, subj, body)
                            log(f"Sent alert for {buoy}: {dist:.1f} m")
                            alerted[buoy] = datetime.now().isoformat()
                        elif dist <= ALERT_THRESHOLD and buoy in alerted:
                            # reset if returned within threshold
                            del alerted[buoy]

                    # missing-transmission
                    current_ids = set(current.index)
                    all_ids     = set(home_df.index)
                    missing     = all_ids - current_ids
                    batch_time  = datetime.fromtimestamp(entry['ts']/1000.0)
                    subj_pref   = f"in batch {batch_time:%Y-%m-%d %H:%M:%S}"
                    for buoy in missing:
                        if buoy not in alerted:
                            subj = f"Alert: Buoy {buoy} missing {subj_pref}"
                            body = (
                                f"Buoy {buoy} did not transmit this batch.\n"
                                f"Batch time: {batch_time:%Y-%m-%d %H:%M:%S}"
                            )
                            send_notification(service, NOTIFY_EMAIL, NOTIFY_EMAIL, subj, body)
                            log(f"Sent missing alert for {buoy}")
                            alerted[buoy] = datetime.now().isoformat()
                        elif buoy in alerted:
                            del alerted[buoy]

                # persist alerts
                with open(ALERT_LOG_FILE, 'w') as f:
                    json.dump(alerted, f, indent=2)

                # append to master CSV
                header = not os.path.exists(MASTER_CSV) or os.path.getsize(MASTER_CSV)==0
                df.to_csv(MASTER_CSV, mode='a', header=header, index=False)
                log(f"Appended {len(df)} rows from {fname}")
                processed = True

        if processed:
            service.users().messages().modify(
                userId='me', id=entry['id'],
                body={'removeLabelIds':['UNREAD'], 'addLabelIds':[label_id]}
            ).execute()
            any_processed = True

    if any_processed:
        # 1) write out latest_positions.csv
        #log("Writing latest positions…")
        df_all = pd.read_csv(MASTER_CSV,
                             parse_dates=['date_UTC'],
                             dtype={'D_number': str},
                             skipinitialspace=True,
                             encoding='utf-8-sig')
        df_all['D_number'] = df_all['D_number'].str.strip()
        latest = (
            df_all
              .sort_values('date_UTC')
              .groupby('D_number', as_index=False)
              .last()
        )
        latest.to_csv(LATEST_CSV_PATH, index=False)
        # 2) regenerate map
        generate_map(home_df)
        # 3) commit & push master CSV and map HTML
        #log("Committing and pushing to git…")
        for f in [MASTER_CSV, MAP_HTML]:
            subprocess.run(['git', 'add', f],
                           cwd=BASE_DIR, check=True)
        subprocess.run(['git', 'commit', '-m',
                        'Update master CSV & map'],
                       cwd=BASE_DIR, check=True)
        subprocess.run(['git', 'push'],
                       cwd=BASE_DIR, check=True)

if __name__ == '__main__':
    fetch_and_append()
