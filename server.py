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
import hmac
import secrets
from functools import wraps
from urllib.parse import urlparse
import requests
import time
import json
import re
from datetime import datetime, timedelta
from threading import Thread, Lock
from flask import (
    Flask, jsonify, request, render_template, make_response,
    session, redirect, url_for
)
import schedule


from flask_socketio import SocketIO, emit

# --- Configuration ---
XML_BASE_URL = "https://scores.tennisticker.de/scoreboard/livescores.aspx?"
DB_NAME = os.getenv("SQLITE_DB_PATH", "casparcg_match_cache.db")

SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "5"))
CURRENT_TOURNAMENT_ID = os.getenv("TOURNAMENT_ID", '7140')
ENABLE_SCRAPER = os.getenv("ENABLE_SCRAPER", "true").strip().lower() in ("1", "true", "yes", "on")
SERVER_PORT = int(os.getenv("PORT", "5000"))

# TennisTicker feed credentials (runtime-changeable via /admin, persisted in DB)
TT_USERID = os.getenv("TT_USERID") or "EFBBCDD3"
TT_CONTRACT = os.getenv("TT_CONTRACT") or "ONSIDEPROD"

# Admin login. If ADMIN_PASSWORD is unset the admin page is disabled.
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

MAX_SETS = 11
# --- End Configuration ---

app = Flask(__name__)
# Session config for the admin login. Provide SECRET_KEY in the environment so
# logins survive restarts; otherwise a random key is generated per boot.
app.secret_key = os.getenv("SECRET_KEY") or secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.getenv("SESSION_COOKIE_SECURE", "false").strip().lower() in ("1", "true", "yes", "on"),
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)
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
                import importlib

                # Prefer psycopg v3 when available; fall back to psycopg2.
                for module_name in ("psycopg", "psycopg2"):
                    try:
                        pg_module = importlib.import_module(module_name)
                        return pg_module.connect(db_url)
                    except Exception:
                        continue

                raise RuntimeError("No compatible PostgreSQL driver found (psycopg or psycopg2).")
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

        # Key/value store for runtime settings changed via the admin page
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        self.conn.commit()

    def get_setting(self, key):
        """Return a persisted setting value, or None if not set."""
        cursor = self.conn.cursor()
        try:
            if self.param_style == 'sqlite':
                cursor.execute("SELECT value FROM app_settings WHERE key=?", (key,))
            else:
                cursor.execute("SELECT value FROM app_settings WHERE key=%s", (key,))
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            print(f"Error reading setting '{key}': {e}")
            return None

    def save_setting(self, key, value):
        """Persist a setting so it survives restarts."""
        cursor = self.conn.cursor()
        try:
            if self.param_style == 'sqlite':
                cursor.execute("""
                    INSERT INTO app_settings (key, value) VALUES (?, ?)
                    ON CONFLICT (key) DO UPDATE SET value=excluded.value
                """, (key, value))
            else:
                cursor.execute("""
                    INSERT INTO app_settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value=excluded.value
                """, (key, value))
            self.conn.commit()
        except Exception as e:
            print(f"Error saving setting '{key}': {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass

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
        with state_lock:
            return (
                f"{XML_BASE_URL}userid={TT_USERID}"
                f"&tournid={CURRENT_TOURNAMENT_ID}"
                f"&contract={TT_CONTRACT}"
            )

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
def load_persisted_settings(mgr):
    """Apply admin settings saved in the DB (they override env defaults)."""
    global CURRENT_TOURNAMENT_ID, TT_USERID, TT_CONTRACT

    with state_lock:
        saved_tournid = mgr.get_setting("tournament_id")
        if saved_tournid and saved_tournid.isdigit():
            CURRENT_TOURNAMENT_ID = saved_tournid
        saved_userid = mgr.get_setting("tt_userid")
        if saved_userid:
            TT_USERID = saved_userid
        saved_contract = mgr.get_setting("tt_contract")
        if saved_contract:
            TT_CONTRACT = saved_contract

    print(f"Settings loaded: tournament={CURRENT_TOURNAMENT_ID}, userid={TT_USERID}, contract={TT_CONTRACT}")


def continuous_scraper_loop():
    """The main loop that runs in a separate thread to continuously scrape the XML."""
    global manager
    manager = XMLCacheManager(DB_NAME)
    load_persisted_settings(manager)
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

    if manager:
        manager.save_setting("tournament_id", candidate)

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


@app.route('/health')
def health_check():
    """Lightweight health endpoint for load balancers and App Runner health checks."""
    return jsonify({"status": "ok"}), 200


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


# ====================================================================
# Admin (login-protected feed credential settings)
# ====================================================================

USERID_PATTERN = re.compile(r'^[A-Za-z0-9]{1,64}$')
CONTRACT_PATTERN = re.compile(r'^[A-Za-z0-9_-]{1,64}$')


def admin_required(f):
    """Redirect to the admin login page unless this session is authenticated."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin_authenticated"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapper


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    """Admin login. Requires the ADMIN_PASSWORD environment variable to be set."""
    error = None

    if not ADMIN_PASSWORD:
        return render_template('admin_login.html', error=None, admin_disabled=True)

    if session.get("admin_authenticated"):
        return redirect(url_for("admin_page"))

    if request.method == 'POST':
        password = request.form.get('password', '')
        if hmac.compare_digest(password, ADMIN_PASSWORD):
            session.permanent = True
            session['admin_authenticated'] = True
            return redirect(url_for("admin_page"))
        # Small fixed delay to slow down brute-force attempts
        time.sleep(0.5)
        error = "Incorrect password."

    return render_template('admin_login.html', error=error, admin_disabled=False)


@app.route('/admin/logout', methods=['POST'])
def admin_logout():
    session.pop('admin_authenticated', None)
    return redirect(url_for("admin_login"))


@app.route('/admin', methods=['GET', 'POST'])
@admin_required
def admin_page():
    """Admin page: change the TennisTicker userid/contract and tournament id at runtime."""
    global TT_USERID, TT_CONTRACT

    message = None
    error = None

    if request.method == 'POST':
        new_userid = request.form.get('tt_userid', '').strip()
        new_contract = request.form.get('tt_contract', '').strip()
        new_tournid = request.form.get('tournament_id', '').strip()

        if not USERID_PATTERN.match(new_userid):
            error = "User ID must be 1-64 letters or digits."
        elif not CONTRACT_PATTERN.match(new_contract):
            error = "Contract must be 1-64 letters, digits, hyphens or underscores."
        else:
            changed = []
            with state_lock:
                if new_userid != TT_USERID:
                    print(f"\n*** TT USERID CHANGED: {TT_USERID} -> {new_userid} ***\n")
                    TT_USERID = new_userid
                    changed.append("User ID")
                if new_contract != TT_CONTRACT:
                    print(f"\n*** TT CONTRACT CHANGED: {TT_CONTRACT} -> {new_contract} ***\n")
                    TT_CONTRACT = new_contract
                    changed.append("Contract")

            if manager:
                manager.save_setting("tt_userid", new_userid)
                manager.save_setting("tt_contract", new_contract)

            if new_tournid:
                ok, msg, _status = update_tournament_id(new_tournid)
                if ok:
                    changed.append("Tournament ID")
                else:
                    error = msg

            if error is None:
                if changed:
                    message = f"Saved: {', '.join(changed)}. The scraper uses the new values on its next fetch."
                    if manager is None:
                        message += " (Warning: scraper disabled, values not persisted to DB.)"
                else:
                    message = "No changes made."

    with state_lock:
        current_userid = TT_USERID
        current_contract = TT_CONTRACT
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    response = make_response(render_template(
        'admin.html',
        tt_userid=current_userid,
        tt_contract=current_contract,
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
# vMix Data Source Endpoints (flat JSON, one row per court)
# ====================================================================

VMIX_LIVE_KEYWORDS = ("IN PROGRESS", "WARMUP", "TEST")
VMIX_SETS_PER_ROW = 5       # per-court endpoint exposes up to 5 sets (tennis best-of-5)
VMIX_SETS_WIDE = 3          # multi-court board shows 3 sets (padel / best-of-3 tennis)
VMIX_MAX_COURTS = 8


def classify_match_status(match):
    """Bucket a cached match into LIVE / UPCOMING / COMPLETED for graphics logic."""
    status = str(match.get("matchstatus", "")).upper()
    if match.get("is_plan") or status == "UPCOMING":
        return "UPCOMING"
    if any(k in status for k in VMIX_LIVE_KEYWORDS):
        return "LIVE"
    if match.get("winner_name") or "COMPLETED" in status or "FINISHED" in status:
        return "COMPLETED"
    # Suspended / rain delay / anything else: pass the raw status through
    return status or "UPCOMING"


def is_set_won(games, opp_games):
    """True if a set score represents a completed, won set (incl. 7-6 TB and 10-point match TB)."""
    try:
        g, o = int(games), int(opp_games)
    except (TypeError, ValueError):
        return False
    if g >= 6 and g - o >= 2:
        return True
    if g == 7 and o in (5, 6):
        return True
    if g >= 10 and g - o >= 2:  # match tiebreak (padel / short formats)
        return True
    return False


def court_sort_key(court):
    """Natural sort: numbered courts first in numeric order, then named courts alphabetically."""
    m = re.search(r"(\d+)", str(court))
    if m:
        return (0, int(m.group(1)), str(court))
    return (1, 0, str(court))


def vmix_flat_row(match, sets_to_include=VMIX_SETS_PER_ROW):
    """
    Flatten one cached match into a single-level dict of strings,
    ready for direct field mapping in vMix Data Sources.
    """
    status = classify_match_status(match)
    is_live = (status == "LIVE")

    # player2serve: 1 = player 1 serving, 2 = player 2 serving (TennisTicker convention)
    serve = str(match.get("player2serve") or "")
    p1_serve = "●" if (is_live and serve == "1") else ""
    p2_serve = "●" if (is_live and serve == "2") else ""

    row = {
        "matchid": str(match.get("matchid") or ""),
        "court": str(match.get("court") or ""),
        "status": status,
        "matchname": str(match.get("matchname") or ""),
        "tournament": str(match.get("tname") or ""),
        "schedtime": str(match.get("schedtime") or ""),
        "winner_name": str(match.get("winner_name") or ""),

        "p1_name": str(match.get("player1") or ""),
        "p2_name": str(match.get("player2") or ""),
        "p1_surname": str(match.get("player1_surname") or ""),
        "p2_surname": str(match.get("player2_surname") or ""),
        "p1_country": str(match.get("player1_country") or ""),
        "p2_country": str(match.get("player2_country") or ""),

        "p1_serve": p1_serve,
        "p2_serve": p2_serve,

        # Point score within the current game ('00', '15', '30', '40', 'AD') – live only
        "p1_points": str(match.get("game1") or "") if is_live else "",
        "p2_points": str(match.get("game2") or "") if is_live else "",
    }

    def as_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    sets_raw = []
    for i in range(1, sets_to_include + 1):
        p1 = match.get(f"set{i}_p1")
        p2 = match.get(f"set{i}_p2")
        present = p1 is not None and p2 is not None
        sets_raw.append((present, as_int(p1), as_int(p2), str(match.get(f"set{i}_tb") or "")))

    # Last set with any games on the board; while live this is the set in play
    # (a brand-new match sits in set 1 at 0-0).
    last_nonzero = 0
    for i, (present, p1, p2, _tb) in enumerate(sets_raw, start=1):
        if present and (p1 > 0 or p2 > 0):
            last_nonzero = i
    current_set = (last_nonzero or (1 if sets_raw[0][0] else 0)) if is_live else 0

    p1_sets_won = 0
    p2_sets_won = 0
    summary_parts = []

    for i, (present, p1, p2, tb) in enumerate(sets_raw, start=1):
        # Show a set only if it has games, or it is the current live set (may be 0-0)
        show = present and ((p1 > 0 or p2 > 0) or i == current_set)

        row[f"p1_set{i}"] = str(p1) if show else ""
        row[f"p2_set{i}"] = str(p2) if show else ""
        row[f"set{i}_tb"] = tb if show else ""

        if show:
            summary_parts.append(f"{p1}-{p2}")
            if is_set_won(p1, p2):
                p1_sets_won += 1
            elif is_set_won(p2, p1):
                p2_sets_won += 1

    row["p1_sets_won"] = str(p1_sets_won)
    row["p2_sets_won"] = str(p2_sets_won)
    row["sets_summary"] = " ".join(summary_parts)
    row["current_set"] = str(current_set) if is_live else ""

    # Games in the set currently being played (bug-style graphics)
    if is_live and current_set:
        row["p1_games"] = row[f"p1_set{current_set}"]
        row["p2_games"] = row[f"p2_set{current_set}"]
    else:
        row["p1_games"] = ""
        row["p2_games"] = ""

    return row


def vmix_blank_row(sets_to_include=VMIX_SETS_PER_ROW):
    """Empty row with the same keys as vmix_flat_row, to pad unused court slots."""
    return {k: "" for k in vmix_flat_row({}, sets_to_include=sets_to_include)}


def select_match_per_court(all_matches):
    """
    Pick the single most relevant match per court:
    live first, then earliest upcoming, then most recently completed.
    """
    by_court = {}
    for m in all_matches:
        court = str(m.get("court") or "").strip()
        if court:
            by_court.setdefault(court, []).append(m)

    selected = {}
    for court, ms in by_court.items():
        live = [x for x in ms if classify_match_status(x) == "LIVE"]
        if live:
            selected[court] = live[0]
            continue
        upcoming = [x for x in ms if classify_match_status(x) == "UPCOMING"]
        if upcoming:
            selected[court] = sorted(upcoming, key=lambda x: str(x.get("schedtime") or ""))[0]
            continue
        completed = [x for x in ms if classify_match_status(x) == "COMPLETED"]
        if completed:
            selected[court] = sorted(completed, key=lambda x: x.get("timestamp") or 0, reverse=True)[0]
            continue
        selected[court] = ms[0]
    return selected


@app.route('/api/v1/vmix', methods=['GET'])
def vmix_datasource():
    """
    vMix Data Source feed: flat JSON array, ONE ROW PER COURT.
    The most relevant match per court is chosen (live > upcoming > completed).
    Optional: ?court=2 to restrict to a single court (exact-token match).
    """
    if manager is None:
        return jsonify([]), 503

    court_filter = (request.args.get("court") or "").strip().lower()

    all_matches = manager.get_latest_data()
    selected = select_match_per_court(all_matches)

    rows = []
    for court in sorted(selected.keys(), key=court_sort_key):
        if court_filter:
            # Token-based match so 'court=1' does not also hit 'Court 11'
            tokens = [t.lower() for t in re.split(r'\W+', court) if t]
            if court_filter not in tokens and court_filter != court.lower():
                continue
        rows.append(vmix_flat_row(selected[court]))

    return jsonify(rows)


@app.route('/api/v1/vmix/wide', methods=['GET'])
def vmix_datasource_wide():
    """
    Multi-court board feed: JSON array with a SINGLE row whose columns are
    flattened per court slot: court1_p1_name, court1_p1_set1, ... court4_p2_points.
    Designed to drive one vMix title showing several courts at once.

    Optional:
      ?courts=4                  number of court slots (1-8, default 4)
      ?order=Court 1,Court 2     explicit court order/selection by name
    """
    if manager is None:
        return jsonify([{}]), 503

    try:
        num_slots = max(1, min(VMIX_MAX_COURTS, int(request.args.get("courts", 4))))
    except ValueError:
        num_slots = 4

    all_matches = manager.get_latest_data()
    selected = select_match_per_court(all_matches)

    order_param = (request.args.get("order") or "").strip()
    if order_param:
        wanted = [c.strip().lower() for c in order_param.split(",") if c.strip()]
        lookup = {c.lower(): c for c in selected}
        court_order = [lookup[w] for w in wanted if w in lookup]
    else:
        court_order = sorted(selected.keys(), key=court_sort_key)

    tournament = ""
    for m in selected.values():
        if m.get("tname"):
            tournament = str(m["tname"])
            break

    row = {"tournament": tournament}
    blank = vmix_blank_row(sets_to_include=VMIX_SETS_WIDE)

    for idx in range(1, num_slots + 1):
        if idx <= len(court_order):
            flat = vmix_flat_row(selected[court_order[idx - 1]], sets_to_include=VMIX_SETS_WIDE)
        else:
            flat = blank
        for key, value in flat.items():
            row[f"court{idx}_{key}"] = value

    return jsonify([row])


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
    # 1. Start background scraper loop unless explicitly disabled
    if ENABLE_SCRAPER:
        scraper_thread = Thread(target=continuous_scraper_loop, daemon=True)
        scraper_thread.start()
        print("Background scraper started in a separate thread.")
    else:
        print("Background scraper disabled (ENABLE_SCRAPER=false).")

    # 2. Start the Flask web server with SocketIO
    print("\n--- Starting Flask API Server with SocketIO ---")
    print(f"   Web/Help Address: http://0.0.0.0:{SERVER_PORT}/")

    # With eventlet as the async_mode, this is now a production-capable server.
    socketio.run(app, host='0.0.0.0', port=SERVER_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScraper and API shutting down...")
        if manager:
            manager.close()
