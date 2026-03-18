"""
File: server.py
Description: Flask + Socket.IO app serving tennis match data with background XML scraping and overlays.
Author: Nathan Silveston
Contact: nathan@nkpa.co.uk | +44 7515 018048
Copyright (c) 2025 Nathan Silveston. All rights reserved.
"""
import xml.etree.ElementTree as ET
import sqlite3
import os
from urllib.parse import urlparse
import requests
import time
import json
import re
from datetime import datetime, timedelta
from threading import Thread, Lock
from flask import Flask, jsonify, request, render_template, make_response
import schedule


from flask_socketio import SocketIO, emit

# --- Configuration ---
XML_BASE_URL = "https://scores.tennisticker.de/scoreboard/livescores.aspx?"
QUERY_STRING = "userid=EFBBCDD3&tournid={tournid}&contract=ONSIDEPROD"
DB_NAME = "casparcg_match_cache.db"

SCRAPE_INTERVAL = 5
CURRENT_TOURNAMENT_ID = '7140'

MAX_SETS = 11
# --- End Configuration ---

app = Flask(__name__)
# Allow all origins for SocketIO for easy testing
socketio = SocketIO(
    app,
    cors_allowed_origins="*",

    async_mode='gevent',  # use eventlet for proper websockets
    logger=True,
    engineio_logger=True,
    manage_session=False
)
manager = None
state_lock = Lock()


# ====================================================================
# XMLCacheManager Class
# ====================================================================
class XMLCacheManager:
    """Manages fetching XML data and caching it in a database (SQLite or Postgres)."""
    def __init__(self, db_name):
        self.conn = self._connect(db_name)
        self.param_style = 'sqlite' if isinstance(self.conn, sqlite3.Connection) else 'postgres'
        self.create_tables()

    def _connect(self, db_name):
        db_url = os.getenv("DATABASE_URL", "")
        if db_url:
            try:
                import psycopg2
                return psycopg2.connect(db_url)
            except Exception as e:
                print(f"Failed to connect to Postgres via DATABASE_URL: {e}. Falling back to SQLite.")
        # SQLite fallback
        return sqlite3.connect(db_name, check_same_thread=False)

    def create_tables(self):
        """
        Creates/updates the 'matches' table with all required columns,
        including winner_name, schedtime, and is_plan.
        """
        global MAX_SETS
        print(f"Creating/Checking database tables with support for up to {MAX_SETS} sets and all XML metadata...")

        cursor = self.conn.cursor()

        # Base table definition (for fresh DBs)
        set_columns = []
        for i in range(1, MAX_SETS + 1):
            set_columns.extend([
                f"set{i}_p1 INTEGER",
                f"set{i}_p2 INTEGER",
                f"set{i}_tb TEXT"
            ])

        set_columns_sql = ", ".join(set_columns)

        metadata_columns = [
            "tournid TEXT", "eventid TEXT", "extmid TEXT", "court TEXT",
            "lshort TEXT", "tname TEXT", "ltouch TEXT", "cam TEXT",
            "cameraurl TEXT", "camerarooturl TEXT", "matchstatusno INTEGER",
            "stats_general INTEGER", "stats_match INTEGER",
            "game1 TEXT", "game2 TEXT", "player2serve INTEGER", "lastservetype INTEGER",
            "schedtime TEXT",              # NEW: scheduled start time for plan
            "is_plan INTEGER"              # NEW: flag for planned matches
        ]

        player_extraction_columns = [
            "player1_country TEXT", "player1_surname TEXT", "player1_full TEXT",
            "player2_country TEXT", "player2_surname TEXT", "player2_full TEXT",
            "winner_name TEXT"
        ]

        all_metadata_sql = ", ".join(metadata_columns + player_extraction_columns)

        # Create table if it does not exist at all
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS matches (
                matchid TEXT PRIMARY KEY,
                player1 TEXT,
                player2 TEXT,
                matchname TEXT,
                matchstatus TEXT,
                winner TEXT,
                timestamp INTEGER,
                sets_played_count INTEGER,
                {all_metadata_sql},
                {set_columns_sql}
            );
        """)
        
        # Create archive table with identical schema plus an archived_at timestamp
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS matches_archive (
                matchid TEXT PRIMARY KEY,
                player1 TEXT,
                player2 TEXT,
                matchname TEXT,
                matchstatus TEXT,
                winner TEXT,
                timestamp INTEGER,
                sets_played_count INTEGER,
                {all_metadata_sql},
                {set_columns_sql},
                archived_at INTEGER
            );
        """)
        self.conn.commit()

        # --- Simple migration: ensure new columns exist on older DBs ---
        existing_cols = []
        try:
            cursor.execute("PRAGMA table_info(matches)")
            existing_cols = [row[1] for row in cursor.fetchall()]
        except Exception:
            # Postgres path: introspect via information_schema
            try:
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='matches'
                """)
                existing_cols = [row[0] for row in cursor.fetchall()]
            except Exception:
                existing_cols = []

        def ensure_column(name, col_type, default_clause=""):
            if name not in existing_cols:
                print(f"Altering table 'matches' to add missing column: {name}")
                try:
                    cursor.execute(f"ALTER TABLE matches ADD COLUMN {name} {col_type} {default_clause};")
                except Exception as e:
                    print(f"Column {name} already exists or error: {e}. Rolling back and continuing...")
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass

        # Ensure schedtime and is_plan exist even if DB was created earlier
        ensure_column("schedtime", "TEXT", "")
        ensure_column("is_plan", "INTEGER", "DEFAULT 0")

        self.conn.commit()

    def archive_completed_previous_day_matches(self):
        """
        Archive completed matches from previous days (before today).
        A match is archived if:
        - winner_name is not empty (match is completed)
        - timestamp indicates it's from a previous day (before today at 00:00:00)
        """
        cursor = self.conn.cursor()
        now = datetime.now()
        today_midnight = int(datetime(now.year, now.month, now.day).timestamp())
        archived_at = int(time.time())

        try:
            # Find completed matches from previous days
            if self.param_style == 'sqlite':
                cursor.execute("""
                    SELECT * FROM matches
                    WHERE winner_name != '' AND winner_name IS NOT NULL
                      AND timestamp < ?
                """, (today_midnight,))
            else:
                cursor.execute("""
                    SELECT * FROM matches
                    WHERE winner_name != '' AND winner_name IS NOT NULL
                      AND timestamp < %s
                """, (today_midnight,))

            rows_to_archive = cursor.fetchall()
            if rows_to_archive:
                cols = [d[0] for d in cursor.description]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Archiving {len(rows_to_archive)} completed previous-day matches...")

                # Insert into archive table
                for row in rows_to_archive:
                    row_dict = dict(zip(cols, row))
                    row_dict['archived_at'] = archived_at

                    columns = list(row_dict.keys())
                    values = list(row_dict.values())

                    cols_sql = ", ".join(columns)
                    if self.param_style == 'sqlite':
                        placeholders = ", ".join(['?'] * len(columns))
                        insert_sql = f"""
                            INSERT OR IGNORE INTO matches_archive ({cols_sql})
                            VALUES ({placeholders})
                        """
                    else:
                        placeholders = ", ".join(['%s'] * len(columns))
                        # Postgres doesn't have INSERT OR IGNORE; use ON CONFLICT
                        insert_sql = f"""
                            INSERT INTO matches_archive ({cols_sql}) VALUES ({placeholders})
                            ON CONFLICT (matchid) DO NOTHING
                        """
                    cursor.execute(insert_sql, tuple(values))

                # Delete from live table
                match_ids_to_delete = [row[0] for row in rows_to_archive]  # matchid is first column
                if self.param_style == 'sqlite':
                    placeholders = ",".join(["?"] * len(match_ids_to_delete))
                else:
                    placeholders = ",".join(["%s"] * len(match_ids_to_delete))
                cursor.execute(f"DELETE FROM matches WHERE matchid IN ({placeholders})", match_ids_to_delete)

                self.conn.commit()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Archived and removed {len(rows_to_archive)} matches from live table.")

        except Exception as e:
            print(f"Error archiving matches: {e}")

    def cleanup_old_archived_matches(self):
        """
        Remove archived matches older than 7 days.
        You can adjust the retention period as needed.
        """
        cursor = self.conn.cursor()
        cutoff_time = int(time.time()) - (7 * 24 * 3600)  # 7 days ago

        try:
            if self.param_style == 'sqlite':
                cursor.execute("DELETE FROM matches_archive WHERE archived_at < ?", (cutoff_time,))
            else:
                cursor.execute("DELETE FROM matches_archive WHERE archived_at < %s", (cutoff_time,))
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Deleted {deleted_count} archived matches older than 7 days.")
            self.conn.commit()
        except Exception as e:
            print(f"Error cleaning up archived matches: {e}")

    def get_full_xml_url(self):
        global CURRENT_TOURNAMENT_ID
        full_query = QUERY_STRING.format(tournid=CURRENT_TOURNAMENT_ID)
        return XML_BASE_URL + full_query

    def fetch_xml_data(self):
        xml_url = self.get_full_xml_url()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching data for TOUR ID {CURRENT_TOURNAMENT_ID}...")
        try:
            response = requests.get(xml_url, timeout=5)
            response.raise_for_status()
            return response.content.decode('utf-8').replace('\xa0', ' ').strip()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}. Using cached data if available.")
            return None

    def get_refresh_interval_from_xml(self, root):
        global SCRAPE_INTERVAL

        try:
            refresh_elem = root.find('config/refresh')
            if refresh_elem is None or not refresh_elem.text:
                return SCRAPE_INTERVAL

            new_interval = int(refresh_elem.text.strip())

            if new_interval > 0:
                return new_interval
            else:
                return SCRAPE_INTERVAL

        except Exception as e:
            print(f"Error parsing refresh interval: {e}. Using default interval.")
            return SCRAPE_INTERVAL

    def parse_player_name(self, full_name):
        """
        Parses a name like 'GUSIC WAN, Ben (GBR)' into Proper Case full name, surname (ALL CAPS), and country.
        Returns (full_extracted_name, surname, country_tag).
        """
        country = ''
        surname = ''
        first_name = ''
        full_extracted_name = ''

        # 1. Extract Country Tag
        country_match = re.search(r'\(([A-Z]{3})\)$', full_name)
        if country_match:
            country = country_match.group(1)
            name_part = full_name[:country_match.start()].strip()
        else:
            name_part = full_name.strip()

        # 2. Extract Surname (part before comma) and First Name (part after comma)
        if ',' in name_part:
            parts = name_part.split(',', 1)

            # Apply ALL CAPS for surname
            surname = parts[0].strip().upper()
            first_name = parts[1].strip().title()

            # 3. Reassemble into "First Name Surname"
            if first_name:
                full_extracted_name = f"{first_name} {surname}"
            else:
                full_extracted_name = surname
        else:
            # Fallback (Apply all caps just in case)
            surname = name_part.upper()
            full_extracted_name = name_part.upper()

        return full_extracted_name, surname, country

    def parse_and_cache_data(self, xml_string):
        """Parses the XML string, checks for changes against DB, and updates ONLY if changed."""
        global SCRAPE_INTERVAL
        global MAX_SETS

        if not xml_string:
            return

        try:
            root = ET.fromstring(xml_string)
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            return

        # --- UPDATE REFRESH INTERVAL (Thread-safe) ---
        with state_lock:
            new_scrape_interval = self.get_refresh_interval_from_xml(root)
            if new_scrape_interval != SCRAPE_INTERVAL:
                print(f"Scrape interval updated from {SCRAPE_INTERVAL}s to {new_scrape_interval}s.")
                SCRAPE_INTERVAL = new_scrape_interval
        # -----------------------------------------------

        # Include LIVE (<match>), COMPLETED (<completed>), and PLANNED (<plan>)
        all_xml_elems = root.findall('match') + root.findall('completed') + root.findall('plan')

        cursor = self.conn.cursor()
        updates_made = 0

        for match_elem in all_xml_elems:
            match_id_elem = match_elem.find('matchid')
            if match_id_elem is None or not match_id_elem.text:
                continue

            match_id = match_id_elem.text.strip()

            # --- HELPER: ROBUST TYPE CONVERSION ---
            def get_text_or_default(elem_name, default=''):
                elem = match_elem.find(elem_name)
                text = elem.text.strip().replace('\xa0', '').strip() if elem is not None and elem.text else ''

                if isinstance(default, int):
                    try:
                        return int(text)
                    except (ValueError, TypeError):
                        return default

                if not text and default != '':
                    return default

                return text

            # --- Detect match type ---
            is_completed = (match_elem.tag == "completed")
            is_live = (match_elem.tag == "match")
            is_plan = (match_elem.tag == "plan")

            # --- Status handling ---
            status_raw = get_text_or_default('matchstatus', default='').strip()
            win_type = get_text_or_default('wintype', default='').strip()

            if is_completed and not status_raw:
                status_raw = "Completed"

            if is_plan:
                # planned matches are always treated as UPCOMING
                status_raw = "UPCOMING"
            
            # If status is WARMUP and win_type is COMPLETED, treat as live warmup
            if status_raw.upper() == "WARMUP" and win_type.upper() == "COMPLETED":
                status_raw = "WARMUP"  # Keep as WARMUP to be treated as live

            # --- sets played ---
            if is_plan:
                sets_played = 0
            else:
                sets_played = 0
                for i in range(MAX_SETS, 0, -1):
                    p1_score = get_text_or_default(f'set{i}1', default=0)
                    p2_score = get_text_or_default(f'set{i}2', default=0)
                    if p1_score > 0 or p2_score > 0:
                        sets_played = i
                        break

            # --- player names ---
            player1_raw_name = get_text_or_default('player1')
            player2_raw_name = get_text_or_default('player2')

            player1_full_extracted, player1_surname, player1_country = self.parse_player_name(player1_raw_name)
            player2_full_extracted, player2_surname, player2_country = self.parse_player_name(player2_raw_name)

            # scheduled time (plan only, but safe to read always)
            scheduled_time = get_text_or_default('schedtime', default='')

            # winner code / name
            winner_code = get_text_or_default('winner', default='')
            winner_name = ""
            if is_plan:
                winner_code = ''
                winner_name = ''
            else:
                if winner_code == '1':
                    winner_name = player1_full_extracted
                elif winner_code == '2':
                    winner_name = player2_full_extracted

            # --- Construct full candidate data dict ---
            candidate_data = {
                'matchid': match_id,

                # player1/player2 stores the Proper Cased full extracted name
                'player1': player1_full_extracted,
                'player2': player2_full_extracted,

                # Full Raw Name
                'player1_full': player1_raw_name,
                'player2_full': player2_raw_name,

                # Extracted fields
                'player1_country': player1_country,
                'player1_surname': player1_surname,  # ALL CAPS
                'player2_country': player2_country,
                'player2_surname': player2_surname,  # ALL CAPS

                'matchname': get_text_or_default('matchname'),
                'matchstatus': status_raw if status_raw else 'UPCOMING',
                'winner': winner_code,
                'winner_name': winner_name,
                'sets_played_count': sets_played,

                # Metadata
                'tournid': get_text_or_default('tournid'),
                'eventid': get_text_or_default('eventid'),
                'extmid': get_text_or_default('extmid'),
                'court': get_text_or_default('court'),
                'lshort': get_text_or_default('lshort'),
                'tname': get_text_or_default('tname'),
                'ltouch': get_text_or_default('ltouch'),
                'cam': get_text_or_default('cam'),
                'cameraurl': get_text_or_default('cameraurl'),
                'camerarooturl': get_text_or_default('camerarooturl'),
                'matchstatusno': get_text_or_default('matchstatusno', default=0),
                'stats_general': get_text_or_default('stats', default=0),
                'stats_match': get_text_or_default('stats', default=0),
                'game1': get_text_or_default('game1', default='0'),
                'game2': get_text_or_default('game2', default='0'),
                'player2serve': get_text_or_default('player2serve', default=0),
                'lastservetype': get_text_or_default('lastservetype', default=0),

                'schedtime': scheduled_time,
                'is_plan': 1 if is_plan else 0
            }

            # Add Set Scores to candidate_data
            for i in range(1, MAX_SETS + 1):
                candidate_data[f"set{i}_p1"] = get_text_or_default(f'set{i}1', default=0)
                candidate_data[f"set{i}_p2"] = get_text_or_default(f'set{i}2', default=0)

                # --- NORMALISE TIE-BREAK VALUES ---
                tb = get_text_or_default(f"set{i}tb", default="")
                # Treat "0", "00", None as "no tiebreak"
                if tb in ("0", "00", None):
                    tb = ""
                candidate_data[f"set{i}_tb"] = tb

            # --- 4. CHECK FOR CHANGES ---
            if self.param_style == 'sqlite':
                cursor.execute("SELECT * FROM matches WHERE matchid=?", (match_id,))
            else:
                cursor.execute("SELECT * FROM matches WHERE matchid=%s", (match_id,))
            row = cursor.fetchone()

            should_update = False

            if row is None:
                should_update = True
            else:
                cols = [d[0] for d in cursor.description]
                existing_data = dict(zip(cols, row))

                for key, new_val in candidate_data.items():
                    existing_val = existing_data.get(key)
                    if existing_val != new_val:
                        should_update = True
                        break

            # --- 5. EXECUTE DB WRITE ONLY IF CHANGED ---
            if should_update:
                candidate_data['timestamp'] = int(time.time())

                columns = list(candidate_data.keys())
                values = list(candidate_data.values())

                cols_sql = ", ".join(columns)
                if self.param_style == 'sqlite':
                    placeholders = ", ".join(['?'] * len(columns))
                else:
                    placeholders = ", ".join(['%s'] * len(columns))
                update_sql = ", ".join([f"{col}=excluded.{col}" for col in columns if col != 'matchid'])

                # Upsert syntax differs; for Postgres use ON CONFLICT, for SQLite it's also supported
                sql = f"""
                    INSERT INTO matches ({cols_sql}) VALUES ({placeholders})
                    ON CONFLICT (matchid) DO UPDATE SET {update_sql};
                """

                cursor.execute(sql, tuple(values))
                updates_made += 1

        self.conn.commit()

        # --- 6b. REMOVE stale planned matches not present in this XML any more ---
        try:
            match_ids_in_xml = set()
            for elem in all_xml_elems:
                mid_elem = elem.find("matchid")
                if mid_elem is not None and mid_elem.text:
                    match_ids_in_xml.add(mid_elem.text.strip())

            cursor.execute("SELECT matchid FROM matches WHERE is_plan=1")
            existing_plan_rows = cursor.fetchall()

            for (mid,) in existing_plan_rows:
                if mid not in match_ids_in_xml:
                    print(f"Removing stale planned match {mid} from DB (no longer present in XML).")
                    if self.param_style == 'sqlite':
                        cursor.execute("DELETE FROM matches WHERE matchid=?", (mid,))
                    else:
                        cursor.execute("DELETE FROM matches WHERE matchid=%s", (mid,))

            self.conn.commit()
        except Exception as e:
            print(f"Error cleaning stale planned matches: {e}")

        # --- 7. EMIT SOCKETIO UPDATE IF DATA CHANGED ---
        if updates_made > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Parsed {len(all_xml_elems)} matches (live/completed/plan). "
                  f"Updated {updates_made} changed records. Emitting SocketIO update.")

            # Get all latest data for client-side filtering
            all_latest_data = self.get_latest_data()

            # Emit to all connected clients on the default namespace
            socketio.emit('live_updates', {
                "timestamp": datetime.now().strftime('%H:%M:%S'),
                "live_matches": all_latest_data
            })

        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Parsed {len(all_xml_elems)} matches. No changes detected.")

    def get_latest_data(self, court_number=None):
        """
        Retrieves all match data from the cache and filters set columns for output.
        Optionally filters by court number.
        """
        cursor = self.conn.cursor()

        # Build the SQL query with optional court filter
        sql = "SELECT * FROM matches"
        params = []

        if court_number:
            # We use LIKE for flexibility, assuming court names are often "Court X"
            if self.param_style == 'sqlite':
                sql += " WHERE court LIKE ?"
                params.append(f"%{court_number}%")
            else:
                sql += " WHERE court LIKE %s"
                params.append(f"%{court_number}%")

        sql += " ORDER BY timestamp DESC"

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cols = [description[0] for description in cursor.description]

        matches_list = []
        for row in rows:
            match_dict = dict(zip(cols, row))

            # Ensure we have an integer for sets_to_keep (DB NULLs may give None)
            sets_to_keep = int(match_dict.get('sets_played_count') or 0)

            filtered_match_dict = {}

            for key, value in match_dict.items():
                # --- Handle scoring display formats for 'game1' and 'game2' ---
                if key in ['game1', 'game2']:
                    # 1. Format '0' to '00'
                    if value == '0':
                        value = '00'
                    # 2. Standardize 'A' or 'AD' to 'AD'
                    elif value in ['A', 'AD']:
                        value = 'AD'

                filtered_match_dict[key] = value

            # Process set scores (already normalised when written)
            for i in range(1, MAX_SETS + 1):
                p1_key = f"set{i}_p1"
                p2_key = f"set{i}_p2"
                tb_key = f"set{i}_tb"

                if p1_key in match_dict:
                    if i <= sets_to_keep:
                        filtered_match_dict[p1_key] = match_dict[p1_key]
                        filtered_match_dict[p2_key] = match_dict[p2_key]
                        filtered_match_dict[tb_key] = match_dict[tb_key]

            matches_list.append(filtered_match_dict)

        # Ensure deterministic ordering by matchid so JSON rows stay in the same positions
        try:
            sorted_matches = sorted(matches_list, key=lambda x: str(x.get('matchid', '')))
        except Exception:
            # Fallback to original order if something unexpected occurs
            sorted_matches = matches_list

        return sorted_matches

    def close(self):
        self.conn.close()


# ====================================================================
# Background Scraper Thread
# ====================================================================
def continuous_scraper_loop():
    """The main loop that runs in a separate thread to continuously scrape the XML."""
    global manager
    manager = XMLCacheManager(DB_NAME)
    print("\n--- Scraper Loop Starting ---")

    # Schedule archiving and cleanup at midnight
    schedule.every().day.at("00:00").do(manager.archive_completed_previous_day_matches)
    schedule.every().day.at("00:05").do(manager.cleanup_old_archived_matches)

    while True:
        # Run scheduled tasks
        schedule.run_pending()

        xml_data = manager.fetch_xml_data()
        if xml_data:
            manager.parse_and_cache_data(xml_data)

        # Emit a simple UTC time heartbeat for the dashboard clock
        try:
            # Emit heartbeat to all clients
            socketio.emit(
                'server_time_utc',
                {"time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
            )
        except Exception as e:
            print(f"Error emitting server_time_utc: {e}")

        with state_lock:
            sleep_time = SCRAPE_INTERVAL
        time.sleep(sleep_time)


# ====================================================================
# Helper: Resolve match for overlays
# ====================================================================
def resolve_match(matchid, auto_single_live=False, fallback_any=False):
    """
    Resolve a match object from the cache:

    - If matchid is provided and found → return that.
    - Else if auto_single_live=True and exactly ONE live match → return that.
    - Else if fallback_any=True → return first live; if none, first overall.
    - Else → return None.
    """
    if manager is None:
        return None

    all_matches = manager.get_latest_data()

    # 1) Explicit matchid
    if matchid:
        m = next((x for x in all_matches if str(x.get("matchid")) == str(matchid)), None)
        if m:
            return m

    # 2) Consider live matches
    live = [
        x for x in all_matches
        if "IN PROGRESS" in str(x.get("matchstatus", "")).upper()
        or "TEST" in str(x.get("matchstatus", "")).upper()
        or "WARMUP" in str(x.get("matchstatus", "")).upper()
    ]

    if auto_single_live:
        if len(live) == 1:
            return live[0]
        # If multiple live, we return None (so you deliberately pick a matchid)
        if len(live) > 1 and not fallback_any:
            return None

    if fallback_any:
        if live:
            return live[0]
        if all_matches:
            return all_matches[0]

    return None


# ====================================================================
# Flask API Endpoints
# ====================================================================

def update_tournament_id(new_tour_id):
    """Validate and update the tracked tournament id in a thread-safe way."""
    global CURRENT_TOURNAMENT_ID

    candidate = (new_tour_id or "").strip()
    if not candidate.isdigit():
        return False, "Tournament ID must be a numeric value.", 400

    with state_lock:
        if candidate != CURRENT_TOURNAMENT_ID:
            print(f"\n*** TOURNAMENT ID CHANGED: {CURRENT_TOURNAMENT_ID} -> {candidate} ***\n")
            CURRENT_TOURNAMENT_ID = candidate

    return True, f"Scraper is now tracking Tournament ID: {candidate}", 200

@app.route('/', methods=['GET'])
def index():
    """API Index: Renders the static HTML page with SocketIO connection for live updates."""
    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    response = make_response(render_template('index.html', tournament_id=tour_id, scrape_interval=interval))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/live_matches')
def live_matches():
    """Live matches page: displays current playing matches, falls back to upcoming when no live matches."""
    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    response = make_response(render_template('live_matches.html', tournament_id=tour_id, scrape_interval=interval))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/results')
def results():
    """Results page: displays all completed matches."""
    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    response = make_response(render_template('results.html', tournament_id=tour_id, scrape_interval=interval))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/help')
def help_page():
    return render_template("help.html")


@app.route('/config', methods=['GET', 'POST'])
def config_page():
    """Configuration page for runtime settings such as tournament id."""
    message = None
    error = None

    if request.method == 'POST':
        new_tour_id = request.form.get('tournament_id', '').strip()
        ok, msg, _status = update_tournament_id(new_tour_id)
        if ok:
            message = msg
        else:
            error = msg

    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    response = make_response(render_template(
        'config.html',
        tournament_id=tour_id,
        scrape_interval=interval,
        message=message,
        error=error
    ))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/api/v1/scores', methods=['GET'])
@app.route('/api/v1/scores/court/<court_number>', methods=['GET'])
def get_all_scores(court_number=None):
    """API endpoint to return all cached match data, optionally filtered by court number."""
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503

    try:
        match_data = manager.get_latest_data(court_number=court_number)

        with state_lock:
            current_tour_id = CURRENT_TOURNAMENT_ID
            refresh_sec = SCRAPE_INTERVAL

        # Compute last_updated from the latest timestamp across all returned matches
        last_updated_str = None
        if match_data:
            try:
                max_ts = max((m.get('timestamp') or 0) for m in match_data)
            except Exception:
                max_ts = 0

            if max_ts:
                last_updated_str = datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')

        return jsonify({
            "status": "success",
            "filter": f"Court: {court_number}" if court_number else "None",
            "tournament_id": current_tour_id,
            "last_updated": last_updated_str,
            "refresh_interval_seconds": refresh_sec,
            "match_count": len(match_data),
            "matches": match_data
        })
    except Exception as e:
        print(f"Error retrieving data from cache: {e}")
        return jsonify({"error": "Failed to retrieve data from cache."}), 500


@app.route('/api/v1/tourid/<new_tour_id>', methods=['POST', 'GET'])
def set_tournament_id(new_tour_id):
    """API endpoint to change the CURRENT_TOURNAMENT_ID that the scraper tracks."""
    ok, msg, status_code = update_tournament_id(new_tour_id)
    if not ok:
        return jsonify({"error": msg}), status_code

    return jsonify({
        "status": "success",
        "message": msg,
        "new_tournament_id": str(new_tour_id).strip()
    })


@app.route('/api/v1/match/<match_id>', methods=['GET'])
def get_single_match(match_id):
    """API endpoint to return a single match by its ID."""
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503

    all_matches = manager.get_latest_data()

    for match in all_matches:
        if str(match.get('matchid')) == str(match_id):
            return jsonify({
                "status": "success",
                "match": match
            })

    return jsonify({"error": f"Match with ID '{match_id}' not found."}), 404


# ====================================================================
# Caspar / vMix Overlays
# ====================================================================

@app.route('/caspar/bug/', methods=['GET'])
def caspar_bug():
    """
    Render the CasparCG/vMix bug overlay.
    Behaviour:
      - If ?matchid=XXX → use that match (if found).
      - Else if exactly ONE live match → use that.
      - Else → no match data (template can handle debug/empty state).
    """
    matchid = request.args.get("matchid")
    debug_mode = request.args.get("debug", "0") == "1"

    # auto_single_live=True, fallback_any=False for bug
    match_data = resolve_match(matchid, auto_single_live=True, fallback_any=False)

    return render_template(
        "caspar_bug.html",
        match_data=match_data,
        debug_mode=debug_mode
    )


@app.route('/caspar/scoreboard/', methods=['GET'])
def caspar_scoreboard():
    """
    Scoreboard overlay for CasparCG / vMix.
    Behaviour:
      - If ?matchid=XXX → use that match (if found).
      - Else if exactly ONE live match → use that.
      - Else if multiple or none → fall back to first live, then first overall.
    """
    matchid = request.args.get("matchid")
    debug_mode = request.args.get("debug", "0") == "1"

    # Slightly more forgiving: show *something* if possible.
    match_data = resolve_match(matchid, auto_single_live=True, fallback_any=True)

    return render_template(
        "scoreboard.html",
        match_data=match_data,
        debug_mode=debug_mode
    )


# ====================================================================
# SocketIO Handlers
# ====================================================================

@socketio.on('connect')
def test_connect():
    """Sends the current match status immediately upon connection."""
    print(f"Client connected: {request.sid}")

    if manager:
        try:
            all_matches = manager.get_latest_data()

            # IMPORTANT: send ALL matches; frontend will filter live/completed/upcoming
            emit('live_updates', {
                "timestamp": datetime.now().strftime('%H:%M:%S'),
                "live_matches": all_matches
            }, room=request.sid)

            print(f"Sent initial status of {len(all_matches)} matches to new client: {request.sid}")

        except Exception as e:
            print(f"Error sending initial status to new client: {e}")


@socketio.on('test_update_request')
def handle_test_update(data):
    """Responds to the test button click by sending a dummy live update to all clients."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Received test update request from client. Emitting test update.")

    # Construct a simple, distinct test message to confirm the path works
    test_update_data = {
        "timestamp": datetime.now().strftime('%H:%M:%S (TEST)'),
        "live_matches": [
            {
                "matchid": "TEST_001",
                "matchname": "TEST MATCH - FORCE UPDATE",
                "matchstatus": "(TESTING)",
                # Note: Test data uses 00 for zero and AD for advantage
                "player1": "Test Player A",
                "player2": "Test Player B",
                "game1": "AD",
                "game2": "00",
                "court": "TEST COURT 99",
                "winner_name": "",
                "schedtime": "Starting at 10:00",
                "is_plan": 1
            }
        ]
    }

    # Emit the test data back to all clients
    # Emit test data to all connected clients
    socketio.emit('live_updates', test_update_data)


@socketio.on('disconnect')
def test_disconnect():
    print(f"Client disconnected: {request.sid}")


# ====================================================================
# Main Execution Block
# ====================================================================
def main():
    """Starts the scraper thread and the Flask web server using SocketIO."""
    # 1. Start the continuous scraping loop in a separate thread
    scraper_thread = Thread(target=continuous_scraper_loop, daemon=True)
    scraper_thread.start()
    print("Background scraper started in a separate thread.")

    # 2. Start the Flask web server with SocketIO
    print("\n--- Starting Flask API Server with SocketIO ---")
    print(f"   Web/Help Address: http://0.0.0.0:5000/")

    # With eventlet as the async_mode, this is now a production-capable server.
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScraper and API shutting down...")
        if manager:
            manager.close()
