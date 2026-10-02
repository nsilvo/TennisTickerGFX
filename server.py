"""
File: server.py
Description: Flask + Socket.IO app serving tennis match data with background XML scraping and overlays.
Author: Nathan Silveston
Contact: nathan@nkpa.co.uk | +44 7515 018048
Copyright (c) 2025 Nathan Silveston. All rights reserved.
"""
# Must run before any other stdlib/network imports so sockets, threading, etc.
# cooperate with the gevent hub used by async_mode='gevent' below.
from gevent import monkey
monkey.patch_all()

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
from werkzeug.utils import secure_filename
from flask import (
    Flask, jsonify, request, render_template, make_response,
    session, redirect, url_for
)
import schedule


from flask_socketio import SocketIO, emit, join_room

# --- Configuration ---
XML_BASE_URL = "https://scores.tennisticker.de/scoreboard/livescores.aspx?"
DB_NAME = os.getenv("SQLITE_DB_PATH", "casparcg_match_cache.db")

SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "5"))
CURRENT_TOURNAMENT_ID = os.getenv("TOURNAMENT_ID", '13')
ENABLE_SCRAPER = os.getenv("ENABLE_SCRAPER", "true").strip().lower() in ("1", "true", "yes", "on")
SERVER_PORT = int(os.getenv("PORT", "5000"))

# TennisTicker feed credentials (runtime-changeable via /admin, persisted in DB)
TT_USERID = os.getenv("TT_USERID") or "33925432PS2018"
TT_CONTRACT = os.getenv("TT_CONTRACT") or "SBPSID"

# Admin login. If ADMIN_PASSWORD is unset the admin page is disabled.
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

MAX_SETS = 11
MAX_STREAM_SLOTS = 4
DEFAULT_STREAM_COUNT = 2
DEFAULT_STREAM_LAYOUT = "side-by-side"
ALLOWED_STREAM_LAYOUTS = {"single", "side-by-side", "grid-2x2"}
DEFAULT_STREAM_COURTS = ["5", "6", "7", "8"]
DEFAULT_STREAM_URLS = ["", "", "", ""]
BUG_LOGO_UPLOAD_DIR = os.path.join("static", "uploads", "caspar_bug")
BUG_LOGO_SETTING_KEY = "caspar_bug_logo"
ALLOWED_BUG_LOGO_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
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
# Verbose Socket.IO packet logging (very noisy with many overlay boxes; off by default)
SOCKETIO_DEBUG = os.getenv("SOCKETIO_DEBUG", "false").strip().lower() in ("1", "true", "yes", "on")

socketio = SocketIO(
    app,
    cors_allowed_origins="*",

    async_mode='gevent',
    logger=SOCKETIO_DEBUG,
    engineio_logger=SOCKETIO_DEBUG,
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
        # Serializes all cursor/commit calls: self.conn is shared between the
        # background scraper thread and Flask request handlers.
        self.db_lock = Lock()
        self._latest_cache = None  # cached result of _load_all_matches(), cleared on any write
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
        # SQLite fallback. timeout= gives writers/readers a grace period instead of
        # raising "database is locked" immediately; WAL lets reads proceed while the
        # scraper thread writes, since both share this single connection.
        conn = sqlite3.connect(db_name, check_same_thread=False, timeout=10)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception as e:
            print(f"Could not set SQLite PRAGMAs: {e}")
        return conn

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

        # Player bios entered by production staff (commentator spotter data).
        # player_key is the normalised upper-case name without country codes.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_bios (
                player_key TEXT PRIMARY KEY,
                display_name TEXT,
                country TEXT,
                born TEXT,
                plays TEXT,
                hometown TEXT,
                career TEXT,
                notes TEXT,
                lta_url TEXT,
                updated_at INTEGER
            );
        """)
        # Migration for DBs created before lta_url existed
        try:
            cursor.execute("ALTER TABLE player_bios ADD COLUMN lta_url TEXT")
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass

        # Score progression history, one row per score change (for commentary graphs)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS match_history (
                matchid TEXT,
                ts INTEGER,
                score TEXT,
                game1 TEXT,
                game2 TEXT,
                player2serve INTEGER,
                matchstatus TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_history_mid ON match_history (matchid, ts);")

        self.conn.commit()

    def get_setting(self, key):
        """Return a persisted setting value, or None if not set."""
        with self.db_lock:
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

    def get_all_settings(self):
        """Return every persisted setting as a dict in a single round trip."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT key, value FROM app_settings")
                return {key: value for key, value in cursor.fetchall()}
            except Exception as e:
                print(f"Error reading settings: {e}")
                return {}

    def save_setting(self, key, value):
        """Persist a setting so it survives restarts."""
        with self.db_lock:
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

    # ----------------------------------------------------------------
    # Player bios (commentator spotter data entered via /players)
    # ----------------------------------------------------------------

    PLAYER_BIO_FIELDS = ("display_name", "country", "born", "plays", "hometown", "career", "notes", "lta_url")

    def get_player_bio(self, player_key):
        """Return the saved bio dict for a player key, or None."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                if self.param_style == 'sqlite':
                    cursor.execute("SELECT * FROM player_bios WHERE player_key=?", (player_key,))
                else:
                    cursor.execute("SELECT * FROM player_bios WHERE player_key=%s", (player_key,))
                row = cursor.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cursor.description]
                return dict(zip(cols, row))
            except Exception as e:
                print(f"Error reading player bio '{player_key}': {e}")
                return None

    def get_all_player_bios(self):
        """Return every saved player bio keyed by player_key."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT * FROM player_bios")
                cols = [d[0] for d in cursor.description]
                return {row[cols.index('player_key')]: dict(zip(cols, row)) for row in cursor.fetchall()}
            except Exception as e:
                print(f"Error reading player bios: {e}")
                return {}

    def save_player_bio(self, player_key, fields):
        """Insert or update a player bio. `fields` maps bio column -> value."""
        values = {col: str(fields.get(col) or '').strip() for col in self.PLAYER_BIO_FIELDS}
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cols = ", ".join(("player_key",) + self.PLAYER_BIO_FIELDS + ("updated_at",))
                ph = "?" if self.param_style == 'sqlite' else "%s"
                placeholders = ", ".join([ph] * (len(self.PLAYER_BIO_FIELDS) + 2))
                update_sql = ", ".join(
                    f"{c}=excluded.{c}" for c in self.PLAYER_BIO_FIELDS + ("updated_at",)
                )
                cursor.execute(f"""
                    INSERT INTO player_bios ({cols}) VALUES ({placeholders})
                    ON CONFLICT (player_key) DO UPDATE SET {update_sql}
                """, (player_key, *[values[c] for c in self.PLAYER_BIO_FIELDS], int(time.time())))
                self.conn.commit()
                return True
            except Exception as e:
                print(f"Error saving player bio '{player_key}': {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                return False

    def get_match_history(self, matchid):
        """Score progression rows for one match, oldest first (for commentary graphs)."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                if self.param_style == 'sqlite':
                    cursor.execute(
                        "SELECT ts, score, game1, game2, player2serve, matchstatus "
                        "FROM match_history WHERE matchid=? ORDER BY ts ASC", (matchid,))
                else:
                    cursor.execute(
                        "SELECT ts, score, game1, game2, player2serve, matchstatus "
                        "FROM match_history WHERE matchid=%s ORDER BY ts ASC", (matchid,))
                return [
                    {"ts": r[0], "score": r[1], "game1": r[2], "game2": r[3],
                     "serve": r[4], "matchstatus": r[5]}
                    for r in cursor.fetchall()
                ]
            except Exception as e:
                print(f"Error reading match history for '{matchid}': {e}")
                return []

    def get_archived_matches(self):
        """Return all archived matches as dicts (for player tournament records)."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT * FROM matches_archive")
                cols = [d[0] for d in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
            except Exception as e:
                print(f"Error reading archived matches: {e}")
                return []

    def archive_completed_previous_day_matches(self):
        """
        Archive completed matches from previous days (before today).
        A match is archived if:
        - winner_name is not empty (match is completed)
        - timestamp indicates it's from a previous day (before today at 00:00:00)
        """
        now = datetime.now()
        today_midnight = int(datetime(now.year, now.month, now.day).timestamp())
        archived_at = int(time.time())

        with self.db_lock:
            cursor = self.conn.cursor()
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
                    self._latest_cache = None
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Archived and removed {len(rows_to_archive)} matches from live table.")

            except Exception as e:
                print(f"Error archiving matches: {e}")

    def cleanup_old_archived_matches(self):
        """
        Remove archived matches older than 7 days.
        You can adjust the retention period as needed.
        """
        cutoff_time = int(time.time()) - (7 * 24 * 3600)  # 7 days ago

        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                if self.param_style == 'sqlite':
                    cursor.execute("DELETE FROM matches_archive WHERE archived_at < ?", (cutoff_time,))
                    cursor.execute("DELETE FROM match_history WHERE ts < ?", (cutoff_time,))
                else:
                    cursor.execute("DELETE FROM matches_archive WHERE archived_at < %s", (cutoff_time,))
                    cursor.execute("DELETE FROM match_history WHERE ts < %s", (cutoff_time,))
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

        updates_made, changed_match_ids = self._write_matches_to_db(all_xml_elems)

        # --- 7. EMIT SOCKETIO UPDATE IF DATA CHANGED ---
        if updates_made > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Parsed {len(all_xml_elems)} matches (live/completed/plan). "
                  f"Updated {updates_made} changed records. Emitting SocketIO updates.")

            all_latest_data = self.get_latest_data()
            now_str = datetime.now().strftime('%H:%M:%S')

            # Dashboards (room 'dashboard') get the full list...
            socketio.emit('live_updates', {
                "timestamp": now_str,
                "live_matches": all_latest_data
            }, to='dashboard')

            # ...while each overlay box only receives its own match/court delta.
            by_id = {str(m.get('matchid')): m for m in all_latest_data}
            for mid in changed_match_ids:
                m = by_id.get(str(mid))
                if not m:
                    continue
                payload = {"timestamp": now_str, "match": m}
                socketio.emit('match_update', payload, to=f"match_{mid}")
                court = str(m.get('court') or '').strip()
                if court:
                    socketio.emit('match_update', payload, to=f"court_{court}")

        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Parsed {len(all_xml_elems)} matches. No changes detected.")

    def _write_matches_to_db(self, all_xml_elems):
        """Upserts parsed matches and prunes stale planned matches. Returns (updates_made, changed_match_ids)."""
        updates_made = 0
        changed_match_ids = []
        removed_stale = False

        with self.db_lock:
            cursor = self.conn.cursor()

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
                score_changed = False
                score_keys = ['matchstatus', 'game1', 'game2', 'player2serve'] + \
                    [f"set{i}_p{p}" for i in range(1, MAX_SETS + 1) for p in (1, 2)]

                if row is None:
                    should_update = True
                    score_changed = True
                else:
                    cols = [d[0] for d in cursor.description]
                    existing_data = dict(zip(cols, row))

                    for key, new_val in candidate_data.items():
                        existing_val = existing_data.get(key)
                        if existing_val != new_val:
                            should_update = True
                            break

                    if should_update:
                        score_changed = any(
                            existing_data.get(k) != candidate_data.get(k) for k in score_keys
                        )

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
                    changed_match_ids.append(match_id)

                    # Record score progression for live matches (commentary graphs)
                    if score_changed and not is_plan:
                        try:
                            ph = "?" if self.param_style == 'sqlite' else "%s"
                            cursor.execute(f"""
                                INSERT INTO match_history
                                    (matchid, ts, score, game1, game2, player2serve, matchstatus)
                                VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                            """, (
                                match_id,
                                candidate_data['timestamp'],
                                match_score_line(candidate_data),
                                str(candidate_data.get('game1') or ''),
                                str(candidate_data.get('game2') or ''),
                                int(candidate_data.get('player2serve') or 0),
                                str(candidate_data.get('matchstatus') or '')
                            ))
                        except Exception as e:
                            print(f"Error recording match history for {match_id}: {e}")

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
                        removed_stale = True

                if removed_stale:
                    self.conn.commit()
            except Exception as e:
                print(f"Error cleaning stale planned matches: {e}")

            if updates_made > 0 or removed_stale:
                self._latest_cache = None

        return updates_made, changed_match_ids

    def get_latest_data(self, court_number=None):
        """
        Retrieves all match data from the cache (cached in-process between writes)
        and filters set columns for output. Optionally filters by court number.
        """
        with self.db_lock:
            if self._latest_cache is None:
                self._latest_cache = self._load_all_matches()
            matches_list = self._latest_cache

        if court_number:
            # Case-insensitive substring match, mirroring the old SQL LIKE %court_number% behaviour
            token = str(court_number).strip().lower()
            matches_list = [
                m for m in matches_list
                if token in str(m.get('court') or '').lower()
            ]

        return matches_list

    def _load_all_matches(self):
        """Full rebuild of the matches list from the DB. Caller must hold db_lock."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM matches ORDER BY timestamp DESC")
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

        # Emit a simple UTC time heartbeat for the dashboard clock (dashboards only)
        try:
            socketio.emit(
                'server_time_utc',
                {"time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')},
                to='dashboard'
            )
        except Exception as e:
            print(f"Error emitting server_time_utc: {e}")

        with state_lock:
            sleep_time = SCRAPE_INTERVAL
        time.sleep(sleep_time)


# ====================================================================
# Helper: Player identity + tournament records (for /players and player API)
# ====================================================================
PLAYER_COUNTRY_RE = re.compile(r'\(([A-Za-z]{2,3})\)')


def normalize_player_key(name):
    """Upper-case player name with country codes and extra whitespace removed."""
    cleaned = PLAYER_COUNTRY_RE.sub('', str(name or ''))
    return re.sub(r'\s+', ' ', cleaned).strip().upper()


def side_player_entries(raw):
    """
    Split a match side ("SKIDELSKY W (GBR) / SMITH A (GBR)") into individual
    players: [{"name": ..., "key": ..., "country": ...}, ...].
    """
    entries = []
    for part in str(raw or '').split('/'):
        country_match = PLAYER_COUNTRY_RE.search(part)
        display = re.sub(r'\s+', ' ', PLAYER_COUNTRY_RE.sub('', part)).strip()
        if display:
            entries.append({
                "name": display,
                "key": display.upper(),
                "country": country_match.group(1).upper() if country_match else ""
            })
    return entries


def match_score_line(match):
    """Compact set-score line for a match, e.g. '6-3 6-4'."""
    parts = []
    sets_played = int(match.get('sets_played_count') or 0)
    for i in range(1, min(sets_played, MAX_SETS) + 1):
        try:
            p1 = int(match.get(f'set{i}_p1') or 0)
            p2 = int(match.get(f'set{i}_p2') or 0)
        except (TypeError, ValueError):
            continue
        if p1 == 0 and p2 == 0:
            continue
        tb = str(match.get(f'set{i}_tb') or '').strip()
        parts.append(f"{p1}-{p2}" + (f"({tb})" if tb else ""))
    return " ".join(parts)


def all_known_matches():
    """Current cached matches plus archived ones, de-duplicated by matchid."""
    if manager is None:
        return []
    matches = list(manager.get_latest_data())
    seen = {str(m.get('matchid')) for m in matches}
    for m in manager.get_archived_matches():
        if str(m.get('matchid')) not in seen:
            matches.append(m)
    return matches


def compute_player_record(player_key):
    """
    Tournament W/L and per-match results for one player, computed from our
    own cached + archived feed data (no external sources).
    """
    wins = 0
    losses = 0
    results = []

    bios = manager.get_all_player_bios() if manager else {}

    def display(p):
        bio = bios.get(p['key'])
        return bio['display_name'] if bio and bio.get('display_name') else p['name']

    for m in all_known_matches():
        side1 = side_player_entries(m.get('player1_full') or m.get('player1'))
        side2 = side_player_entries(m.get('player2_full') or m.get('player2'))
        on1 = any(p['key'] == player_key for p in side1)
        on2 = any(p['key'] == player_key for p in side2)
        if not (on1 or on2):
            continue

        status = classify_match_status(m)
        winner_code = str(m.get('winner') or '').strip()
        result = ""
        if status == "COMPLETED" and winner_code in ("1", "2"):
            won = (winner_code == "1" and on1) or (winner_code == "2" and on2)
            result = "W" if won else "L"
            if won:
                wins += 1
            else:
                losses += 1

        own_side, opp_side = (side1, side2) if on1 else (side2, side1)
        results.append({
            "matchid": str(m.get('matchid') or ''),
            "event": str(m.get('tname') or ''),
            "round": str(m.get('matchname') or ''),
            "court": str(m.get('court') or ''),
            "status": status,
            "result": result,
            "partner": " / ".join(display(p) for p in own_side if p['key'] != player_key),
            "opponent": " / ".join(display(p) for p in opp_side) or "TBC",
            "score": match_score_line(m),
            "schedtime": str(m.get('schedtime') or ''),
            "timestamp": m.get('timestamp') or 0
        })

    results.sort(key=lambda r: r['timestamp'], reverse=True)
    return wins, losses, results


def collect_known_players():
    """
    Every individual player seen in the feed (cache + archive), merged with
    saved bios. Returns {player_key: {"name", "country", "has_bio"}}.
    """
    players = {}
    for m in all_known_matches():
        for raw in (m.get('player1_full') or m.get('player1'),
                    m.get('player2_full') or m.get('player2')):
            for p in side_player_entries(raw):
                if p['key'] in ('TBC', 'BYE', ''):
                    continue
                existing = players.get(p['key'])
                if not existing:
                    players[p['key']] = {"name": p['name'], "country": p['country'], "has_bio": False}
                elif not existing['country'] and p['country']:
                    existing['country'] = p['country']

    if manager:
        for key, bio in manager.get_all_player_bios().items():
            entry = players.setdefault(key, {"name": bio.get('display_name') or key, "country": bio.get('country') or '', "has_bio": True})
            entry['has_bio'] = True
            if bio.get('display_name'):
                entry['name'] = bio['display_name']
    return players


# ====================================================================
# Helper: Resolve match for overlays
# ====================================================================
def normalize_court_number(court_value):
    """Extract the numeric court token from a raw court label."""
    court_match = re.search(r"(\d+)", str(court_value or ""))
    return court_match.group(1) if court_match else ""


def resolve_match(matchid=None, court=None, auto_single_live=False, fallback_any=False):
    """
    Resolve a match object from the cache:

    - If matchid is provided and found → return that.
    - If court is provided, return the best match on that court.
    - Else if auto_single_live=True and exactly ONE live match → return that.
    - Else if fallback_any=True → return first live; if none, first overall.
    - Else → return None.
    """
    if manager is None:
        return None

    all_matches = manager.get_latest_data()

    court_number = normalize_court_number(court)

    if court_number:
        court_matches = [
            x for x in all_matches
            if normalize_court_number(x.get("court")) == court_number
        ]

        if court_matches:
            live = [
                x for x in court_matches
                if "IN PROGRESS" in str(x.get("matchstatus", "")).upper()
                or "TEST" in str(x.get("matchstatus", "")).upper()
                or "WARMUP" in str(x.get("matchstatus", "")).upper()
            ]
            if live:
                return live[0]

            upcoming = [
                x for x in court_matches
                if x.get("is_plan") or "UPCOMING" in str(x.get("matchstatus", "")).upper()
            ]
            if upcoming:
                return sorted(upcoming, key=lambda x: str(x.get("schedtime") or ""))[0]

            completed = [
                x for x in court_matches
                if x.get("winner_name") or "COMPLETED" in str(x.get("matchstatus", "")).upper()
                or "FINISHED" in str(x.get("matchstatus", "")).upper()
            ]
            if completed:
                return sorted(completed, key=lambda x: x.get("timestamp") or 0, reverse=True)[0]

            return court_matches[0]

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


def get_live_stream_config():
    """Read stream layout settings from persisted config (with safe defaults)."""
    stream_count = DEFAULT_STREAM_COUNT
    stream_layout = DEFAULT_STREAM_LAYOUT
    stream_courts = DEFAULT_STREAM_COURTS.copy()
    stream_urls = DEFAULT_STREAM_URLS.copy()

    if manager:
        settings = manager.get_all_settings()

        count_raw = settings.get("stream_count")
        try:
            if count_raw is not None:
                stream_count = int(count_raw)
        except (TypeError, ValueError):
            pass

        layout_raw = (settings.get("stream_layout") or "").strip().lower()
        if layout_raw in ALLOWED_STREAM_LAYOUTS:
            stream_layout = layout_raw

        for idx in range(MAX_STREAM_SLOTS):
            saved_court = (settings.get(f"stream_court_{idx + 1}") or "").strip()
            if saved_court:
                stream_courts[idx] = saved_court
            saved_url = (settings.get(f"stream_url_{idx + 1}") or "").strip()
            if saved_url:
                stream_urls[idx] = saved_url

    stream_count = max(1, min(MAX_STREAM_SLOTS, int(stream_count)))
    if stream_layout not in ALLOWED_STREAM_LAYOUTS:
        stream_layout = DEFAULT_STREAM_LAYOUT

    return {
        "stream_count": stream_count,
        "stream_layout": stream_layout,
        "stream_courts": stream_courts,
        "stream_urls": stream_urls,
    }


def get_available_court_numbers():
    """Return sorted court numbers inferred from cached matches for config dropdowns."""
    default_numbers = sorted({int(c) for c in DEFAULT_STREAM_COURTS if c.isdigit()})
    numbers = set(default_numbers)

    if not manager:
        return [str(n) for n in sorted(numbers)]

    try:
        all_matches = manager.get_latest_data()
        for match in all_matches:
            court_raw = str(match.get("court") or "")
            court_match = re.search(r"(\d+)", court_raw)
            if court_match:
                numbers.add(int(court_match.group(1)))
    except Exception as e:
        print(f"Error collecting court numbers for config: {e}")

    return [str(n) for n in sorted(numbers)]


def save_live_stream_config_from_form(form):
    """Validate and persist stream layout settings from the config page."""
    if not manager:
        return False, "Stream settings can only be saved while the scraper manager is running."

    count_raw = (form.get("stream_count") or "").strip()
    layout_raw = (form.get("stream_layout") or "").strip().lower()

    if not count_raw.isdigit():
        return False, "Stream count must be a numeric value between 1 and 4."

    stream_count = int(count_raw)
    if stream_count < 1 or stream_count > MAX_STREAM_SLOTS:
        return False, "Stream count must be between 1 and 4."

    if layout_raw not in ALLOWED_STREAM_LAYOUTS:
        return False, "Invalid layout selected."

    stream_courts = []
    stream_urls = []
    for idx in range(MAX_STREAM_SLOTS):
        raw_court = (form.get(f"stream_court_{idx + 1}") or "").strip()
        if raw_court and not raw_court.isdigit():
            return False, f"Court selection for stream {idx + 1} must be numeric."
        stream_courts.append(raw_court)

        raw_url = (form.get(f"stream_url_{idx + 1}") or "").strip()
        if raw_url:
            parsed = urlparse(raw_url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                return False, f"Stream {idx + 1} URL must be a valid http(s) URL."
        stream_urls.append(raw_url)

    manager.save_setting("stream_count", str(stream_count))
    manager.save_setting("stream_layout", layout_raw)
    for idx, court_value in enumerate(stream_courts, start=1):
        manager.save_setting(f"stream_court_{idx}", court_value)
    for idx, url_value in enumerate(stream_urls, start=1):
        manager.save_setting(f"stream_url_{idx}", url_value)

    return True, "Saved stream layout, court assignments and stream URLs."


def get_bug_logo_url():
    """Return the uploaded bug logo URL if one has been configured."""
    if not manager:
        return None

    logo_filename = (manager.get_setting(BUG_LOGO_SETTING_KEY) or "").strip()
    if not logo_filename:
        return None

    logo_path = os.path.join(app.root_path, BUG_LOGO_UPLOAD_DIR, logo_filename)
    if not os.path.exists(logo_path):
        return None

    return url_for("static", filename=f"uploads/caspar_bug/{logo_filename}")


def save_bug_logo_upload(uploaded_file):
    """Persist an uploaded logo for the Caspar bug overlay."""
    if not manager:
        return False, "Logo uploads require the scraper manager to be running."

    if uploaded_file is None or not uploaded_file.filename:
        return False, "Choose a logo image to upload."

    filename = secure_filename(uploaded_file.filename)
    if "." not in filename:
        return False, "Logo file must have an image extension."

    extension = filename.rsplit(".", 1)[1].lower()
    if extension not in ALLOWED_BUG_LOGO_EXTENSIONS:
        return False, "Logo must be a PNG, JPG, JPEG, GIF, or WEBP file."

    upload_dir = os.path.join(app.root_path, BUG_LOGO_UPLOAD_DIR)
    os.makedirs(upload_dir, exist_ok=True)

    for existing_name in os.listdir(upload_dir):
        if existing_name.startswith("caspar_bug_logo."):
            try:
                os.remove(os.path.join(upload_dir, existing_name))
            except OSError:
                pass

    stored_filename = f"caspar_bug_logo.{extension}"
    uploaded_file.save(os.path.join(upload_dir, stored_filename))
    manager.save_setting(BUG_LOGO_SETTING_KEY, stored_filename)

    return True, "Saved bug logo image."

@app.route('/', methods=['GET'])
def index():
    """API Index: Renders the static HTML page with SocketIO connection for live updates."""
    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    base_url = request.url_root.rstrip('/')

    response = make_response(render_template(
        'index.html', tournament_id=tour_id, scrape_interval=interval, base_url=base_url
    ))
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

    stream_config = get_live_stream_config()

    response = make_response(render_template(
        'live_matches.html',
        tournament_id=tour_id,
        scrape_interval=interval,
        stream_config=stream_config
    ))
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


@app.route('/api_links')
def api_links_page():
    """Copy/paste API and overlay links, grouped per court (pinned courts shown first)."""
    stream_config = get_live_stream_config()
    pinned_courts = [c for c in stream_config["stream_courts"] if c]

    response = make_response(render_template('api_links.html', pinned_courts=pinned_courts))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/stats')
def stats_page():
    """Commentary screen: live match stats for commentators, optionally focused on one court."""
    court = (request.args.get('court') or '').strip()

    stream_config = get_live_stream_config()
    pinned_courts = [c for c in stream_config["stream_courts"] if c]

    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    response = make_response(render_template(
        'stats.html',
        court=court,
        pinned_courts=pinned_courts,
        tournament_id=tour_id,
        scrape_interval=interval
    ))
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
        form_name = (request.form.get('form_name') or '').strip().lower()

        if form_name == 'stream_settings':
            ok, msg = save_live_stream_config_from_form(request.form)
            if ok:
                message = msg
            else:
                error = msg
        elif form_name == 'bug_logo':
            ok, msg = save_bug_logo_upload(request.files.get('bug_logo'))
            if ok:
                message = msg
            else:
                error = msg
        else:
            new_tour_id = request.form.get('tournament_id', '').strip()
            ok, msg, _status = update_tournament_id(new_tour_id)
            if ok:
                message = msg
            else:
                error = msg

    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL

    stream_config = get_live_stream_config()
    available_courts = get_available_court_numbers()
    bug_logo_url = get_bug_logo_url()

    response = make_response(render_template(
        'config.html',
        tournament_id=tour_id,
        scrape_interval=interval,
        stream_config=stream_config,
        available_courts=available_courts,
        bug_logo_url=bug_logo_url,
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


@app.route('/api/v1/match/<match_id>/history', methods=['GET'])
def get_match_history_api(match_id):
    """Score progression for one match (one point per score change), for graphs."""
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503

    points = manager.get_match_history(str(match_id))
    return jsonify({
        "status": "success",
        "matchid": str(match_id),
        "point_count": len(points),
        "points": points
    })


# ====================================================================
# Player data endpoints (bios entered via /players + computed records)
# ====================================================================

@app.route('/api/v1/players', methods=['GET'])
def api_players_list():
    """List every player seen in the feed with bio availability flags."""
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503

    players = collect_known_players()
    rows = [
        {"player_key": key, "name": info["name"], "country": info["country"], "has_bio": info["has_bio"]}
        for key, info in sorted(players.items())
    ]
    return jsonify({"status": "success", "player_count": len(rows), "players": rows})


@app.route('/api/v1/player/<path:player_name>', methods=['GET'])
def api_player_detail(player_name):
    """
    Flat, graphics-ready JSON for one player: saved bio fields plus the
    tournament W/L record and results computed from our own cached feed data.
    """
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503

    player_key = normalize_player_key(player_name)
    if not player_key:
        return jsonify({"error": "Player name required."}), 400

    bio = manager.get_player_bio(player_key) or {}
    wins, losses, results = compute_player_record(player_key)

    if not bio and not results:
        return jsonify({"error": f"No data for player '{player_key}'."}), 404

    display_name = bio.get('display_name') or player_key.title()
    # Prefer the exact feed casing when we have seen the player in a match
    known = collect_known_players().get(player_key)
    if known and not bio.get('display_name'):
        display_name = known['name']

    last_completed = next((r for r in results if r['result']), None)

    return jsonify({
        "status": "success",
        "player_key": player_key,
        "name": display_name,
        "country": bio.get('country') or (known['country'] if known else ''),
        "born": bio.get('born') or '',
        "plays": bio.get('plays') or '',
        "hometown": bio.get('hometown') or '',
        "career": bio.get('career') or '',
        "notes": bio.get('notes') or '',
        "lta_url": bio.get('lta_url') or '',
        "tournament_wins": wins,
        "tournament_losses": losses,
        "tournament_record": f"{wins}-{losses}",
        "last_result": (
            f"{last_completed['result']} vs {last_completed['opponent']} {last_completed['score']}".strip()
            if last_completed else ''
        ),
        "matches": results
    })


@app.route('/players', methods=['GET', 'POST'])
def players_page():
    """Player bio editor: production staff maintain commentator spotter data."""
    message = None
    error = None

    if request.method == 'POST':
        form_name = (request.form.get('form_name') or '').strip().lower()

        if form_name == 'bulk_names':
            # Bulk full-name entry: save display names without touching other bio fields
            if manager is None:
                error = "Database not ready yet - try again shortly."
            else:
                saved = 0
                for key_raw, name in zip(request.form.getlist('bulk_key'),
                                         request.form.getlist('bulk_name')):
                    key = normalize_player_key(key_raw)
                    name = (name or '').strip()
                    if not key or not name:
                        continue
                    existing = manager.get_player_bio(key) or {}
                    if name == (existing.get('display_name') or ''):
                        continue
                    fields = {c: existing.get(c) or '' for c in manager.PLAYER_BIO_FIELDS}
                    fields['display_name'] = name
                    if manager.save_player_bio(key, fields):
                        saved += 1
                message = f"Saved {saved} full name(s)." if saved else "No name changes to save."
        else:
            player_key = normalize_player_key(request.form.get('player_key') or request.form.get('display_name'))
            if not player_key:
                error = "Player name is required."
            elif manager is None:
                error = "Database not ready yet - try again shortly."
            else:
                fields = {col: request.form.get(col, '') for col in manager.PLAYER_BIO_FIELDS}
                if not fields.get('display_name'):
                    fields['display_name'] = request.form.get('player_key', '').strip()
                if manager.save_player_bio(player_key, fields):
                    message = f"Saved bio for {fields.get('display_name') or player_key}."
                else:
                    error = "Failed to save bio - check the server logs."

    players = collect_known_players() if manager else {}
    bios = manager.get_all_player_bios() if manager else {}

    player_rows = [
        {
            "key": key,
            "name": info["name"],
            "country": info["country"],
            "has_bio": info["has_bio"],
            "bio": bios.get(key) or {}
        }
        for key, info in sorted(players.items(), key=lambda kv: kv[1]["name"].upper())
    ]

    response = make_response(render_template(
        'players.html',
        players=player_rows,
        message=message,
        error=error
    ))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


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
    p1_serve = "1" if (is_live and serve == "1") else ""
    p2_serve = "1" if (is_live and serve == "2") else ""

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
        # Raw serving player indicator: "1" or "2", blank when not live
        "player2serve": serve if (is_live and serve in ("1", "2")) else "",

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

        row[f"p1_set{i}"] = str(p1) if show else "-"
        row[f"p2_set{i}"] = str(p2) if show else "-"
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
    court = request.args.get("court")
    debug_mode = request.args.get("debug", "0") == "1"

    # auto_single_live=True, fallback_any=False for bug
    match_data = resolve_match(matchid=matchid, court=court, auto_single_live=True, fallback_any=False)
    bug_logo_url = get_bug_logo_url()

    return render_template(
        "caspar_bug.html",
        match_data=match_data,
        debug_mode=debug_mode,
        bug_logo_url=bug_logo_url,
        bug_court=normalize_court_number(court or (match_data or {}).get("court"))
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
    """Lightweight connect: clients declare what they need via 'subscribe' / 'subscribe_all'."""
    print(f"Client connected: {request.sid}")


@socketio.on('subscribe')
def handle_subscribe(data):
    """
    Overlay subscription: join per-match and/or per-court rooms so this client
    only receives 'match_update' events for its own match/court.
    Payload: { matchid: "...", court: "..." } (either or both).
    Replies immediately with a snapshot of the requested match.
    """
    data = data or {}
    matchid = str(data.get('matchid') or '').strip()
    court = str(data.get('court') or '').strip()

    # Join exactly ONE room to avoid duplicate events: the court room when the
    # court is known (also catches the next match on the same court), else the
    # specific match room.
    if court:
        join_room(f"court_{court}")
    elif matchid:
        join_room(f"match_{matchid}")

    print(f"Client {request.sid} subscribed to match='{matchid}' court='{court}'")

    if manager and (matchid or court):
        try:
            snapshot = None
            if matchid:
                snapshot = next((x for x in manager.get_latest_data()
                                 if str(x.get('matchid')) == matchid), None)
            if snapshot is None and court:
                court_number = normalize_court_number(court)
                if court_number:
                    snapshot = resolve_match(court=court_number, auto_single_live=True, fallback_any=True)

            if snapshot:
                emit('match_update', {
                    "timestamp": datetime.now().strftime('%H:%M:%S'),
                    "match": snapshot
                })
        except Exception as e:
            print(f"Error sending subscribe snapshot to {request.sid}: {e}")


@socketio.on('subscribe_all')
def handle_subscribe_all(_data=None):
    """
    Dashboard subscription: joins the 'dashboard' room for full live_updates
    broadcasts and the clock heartbeat. Replies with a full snapshot.
    """
    join_room('dashboard')
    print(f"Client {request.sid} subscribed to dashboard (all matches)")

    if manager:
        try:
            all_matches = manager.get_latest_data()
            emit('live_updates', {
                "timestamp": datetime.now().strftime('%H:%M:%S'),
                "live_matches": all_matches
            })
        except Exception as e:
            print(f"Error sending dashboard snapshot to {request.sid}: {e}")


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

    # Emit the test data to dashboard clients
    socketio.emit('live_updates', test_update_data, to='dashboard')


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
