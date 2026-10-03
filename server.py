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
import difflib
import functools
import itertools
from datetime import datetime, timedelta
from threading import Thread, Lock
from flask import (
    Flask, jsonify, request, render_template, make_response,
    session, redirect, url_for
)
import schedule


from flask_socketio import SocketIO, emit, join_room

import manual_scoring

# --- Configuration ---
XML_BASE_URL = "https://scores.tennisticker.de/scoreboard/livescores.aspx?"
DB_NAME = os.getenv("SQLITE_DB_PATH", "casparcg_match_cache.db")

SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "5"))
CURRENT_TOURNAMENT_ID = os.getenv("TOURNAMENT_ID", '13')
# A feed can carry matches from several tournaments; each match has its own <tournid>.
# When non-empty, only matches with these tournids are shown anywhere (set on /config).
MATCH_TOURNID_FILTER = [t.strip() for t in os.getenv("MATCH_TOURNID_FILTER", "").split(",") if t.strip()]
# Minutes a finished match's result stays as its court's vMix / overlay match (0 = move on at once).
# The next match going live, or "next match" for the court, ends the hold early.
RESULT_HOLD_MINUTES = int(os.getenv("RESULT_HOLD_MINUTES", "5"))
released_results = set()   # match ids whose result hold was ended early
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
        # Commentator manual scoring: {matchid: state}; active entries override the feed score
        self.manual_scores = self.load_manual_scores()
        self.player_aliases = self.load_player_aliases()
        # When the feed last changed each match's score (in memory; drives feed-vs-manual freshness)
        self.feed_score_ts = {}

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
                lta_stats TEXT,
                updated_at INTEGER
            );
        """)
        # Migrations for DBs created before lta_url / lta_stats existed. Commit first:
        # on Postgres a failed ALTER's rollback would otherwise undo the CREATEs above.
        self.conn.commit()
        for column in ("lta_url", "lta_stats"):
            try:
                cursor.execute(f"ALTER TABLE player_bios ADD COLUMN {column} TEXT")
                self.conn.commit()
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

        # Uploaded images (e.g. the bug logo) kept in the database so they survive redeploys
        # and work when the app folder is mounted read-only. data is base64 text.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media_files (
                name TEXT PRIMARY KEY,
                content_type TEXT,
                data TEXT,
                updated_at INTEGER
            );
        """)

        # Linked duplicate player entries: alias_key -> the player_key that holds the bio
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_aliases (
                alias_key TEXT PRIMARY KEY,
                player_key TEXT
            );
        """)

        # Commentator manual scoring state (JSON from manual_scoring.py), one row per match
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_scores (
                matchid TEXT PRIMARY KEY,
                active INTEGER,
                state TEXT,
                updated_at INTEGER
            );
        """)

        self.conn.commit()

    # ----------------------------------------------------------------
    # Manual scoring (commentator scores a match point by point)
    # ----------------------------------------------------------------

    def load_manual_scores(self):
        """Active manual scoring states keyed by matchid."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT matchid, state FROM manual_scores WHERE active=1")
                return {str(mid): json.loads(state) for mid, state in cursor.fetchall() if state}
            except Exception as e:
                print(f"Error loading manual scores: {e}")
                return {}

    def save_manual_score(self, matchid, state, active=True):
        """Persist (or deactivate) a match's manual scoring state."""
        matchid = str(matchid)
        if active:
            self.manual_scores[matchid] = state
        else:
            self.manual_scores.pop(matchid, None)
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                ph = "?" if self.param_style == 'sqlite' else "%s"
                cursor.execute(f"""
                    INSERT INTO manual_scores (matchid, active, state, updated_at) VALUES ({ph}, {ph}, {ph}, {ph})
                    ON CONFLICT (matchid) DO UPDATE SET active=excluded.active, state=excluded.state,
                        updated_at=excluded.updated_at
                """, (matchid, 1 if active else 0, json.dumps(state), int(time.time())))
                self.conn.commit()
            except Exception as e:
                print(f"Error saving manual score for {matchid}: {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass

    def get_feed_match(self, matchid):
        """The feed's own row for a match (no manual overlay, no tournament filter), or None."""
        with self.db_lock:
            if self._latest_cache is None:
                self._latest_cache = self._load_all_matches()
            cache = self._latest_cache
        found = next((m for m in cache if str(m.get('matchid')) == str(matchid)), None)
        if found is None and str(matchid).startswith(STAGED_PREFIX):
            found = next((m for m in staged_match_rows(cache)[0] if m['matchid'] == str(matchid)), None)
        return found

    def feed_is_newer(self, matchid):
        """True when the feed changed this match's score after the last manual scoring action."""
        state = self.manual_scores.get(str(matchid))
        if not state or state.get("feed_lock") or str(matchid).startswith(STAGED_PREFIX):
            return False   # (a staged match has no TennisTicker data of its own)
        if manual_scoring.awaiting_confirmation(state):
            return False   # finished result stays on air until the scorer marks it complete
        return self.feed_score_ts.get(str(matchid), 0) > state.get("updated_at", 0)

    def record_history(self, match):
        """Append one score-progression row (commentary graphs) for a match dict."""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                self._insert_history(cursor, match.get('matchid'), int(time.time()), match)
                self.conn.commit()
            except Exception as e:
                print(f"Error recording match history for {match.get('matchid')}: {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass

    def _insert_history(self, cursor, match_id, ts, match):
        ph = "?" if self.param_style == 'sqlite' else "%s"
        cursor.execute(f"""
            INSERT INTO match_history
                (matchid, ts, score, game1, game2, player2serve, matchstatus)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        """, (
            match_id,
            ts,
            match_score_line(match),
            str(match.get('game1') or ''),
            str(match.get('game2') or ''),
            int(match.get('player2serve') or 0),
            str(match.get('matchstatus') or '')
        ))

    def broadcast_matches(self, changed_match_ids):
        """Push the full list to dashboards and each changed match to its overlay rooms."""
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
            # Overlays opened for the staged version of this match keep following it
            for staged_id in staged_ids_for(mid):
                socketio.emit('match_update', payload, to=f"match_{staged_id}")
            court = str(m.get('court') or '').strip()
            if court:
                socketio.emit('match_update', payload, to=f"court_{court}")

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

    # lta_stats is JSON scraped from the LTA profile (records, form, titles)
    PLAYER_BIO_FIELDS = ("display_name", "country", "born", "plays", "hometown", "career", "notes", "lta_url",
                         "lta_stats")

    # ---- uploaded media (stored in the database) ----

    def save_media(self, name, content_type, payload):
        import base64
        ph = "?" if self.param_style == 'sqlite' else "%s"
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(f"""
                    INSERT INTO media_files (name, content_type, data, updated_at) VALUES ({ph}, {ph}, {ph}, {ph})
                    ON CONFLICT (name) DO UPDATE SET content_type=excluded.content_type, data=excluded.data,
                        updated_at=excluded.updated_at
                """, (name, content_type, base64.b64encode(payload).decode('ascii'), int(time.time())))
                self.conn.commit()
                return True
            except Exception as e:
                print(f"Error saving media '{name}': {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                return False

    def get_media(self, name):
        """(content_type, bytes, updated_at) or None."""
        import base64
        ph = "?" if self.param_style == 'sqlite' else "%s"
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(f"SELECT content_type, data, updated_at FROM media_files WHERE name={ph}", (name,))
                row = cursor.fetchone()
                return (row[0], base64.b64decode(row[1]), row[2]) if row else None
            except Exception as e:
                print(f"Error reading media '{name}': {e}")
                return None

    def get_media_version(self, name):
        """updated_at for a stored file (cheap: no image data), or None."""
        ph = "?" if self.param_style == 'sqlite' else "%s"
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(f"SELECT updated_at FROM media_files WHERE name={ph}", (name,))
                row = cursor.fetchone()
                return row[0] if row else None
            except Exception as e:
                print(f"Error reading media version '{name}': {e}")
                return None

    # ---- linked duplicates (aliases) ----

    def load_player_aliases(self):
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT alias_key, player_key FROM player_aliases")
                return {a: k for a, k in cursor.fetchall()}
            except Exception as e:
                print(f"Error loading player aliases: {e}")
                return {}

    def resolve_player_key(self, key):
        """The key that holds this player's bio (follows links from duplicate entries)."""
        seen = set()
        while key in self.player_aliases and key not in seen:
            seen.add(key)
            key = self.player_aliases[key]
        return key

    def aliases_of(self, key):
        return sorted(a for a in self.player_aliases if self.resolve_player_key(a) == key)

    def link_players(self, duplicate_key, keep_key):
        """
        Merge `duplicate_key` into `keep_key`: empty bio fields are filled from the
        duplicate, the duplicate's bio row is removed, and the duplicate key becomes an
        alias so every lookup lands on the kept player. Returns (ok, message).
        """
        keep_key = self.resolve_player_key(keep_key)
        duplicate_key = self.resolve_player_key(duplicate_key)
        if not keep_key or not duplicate_key or keep_key == duplicate_key:
            return False, "Choose two different players to link."
        keep = self.get_player_bio(keep_key) or {}
        dup = self.get_player_bio(duplicate_key) or {}
        merged = {c: (keep.get(c) or dup.get(c) or '') for c in self.PLAYER_BIO_FIELDS}
        if not merged.get('display_name'):
            merged['display_name'] = keep_key.title()
        self.save_player_bio(keep_key, merged)
        ph = "?" if self.param_style == 'sqlite' else "%s"
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(f"DELETE FROM player_bios WHERE player_key={ph}", (duplicate_key,))
                # The duplicate and anything already linked to it now point at the kept player
                cursor.execute(f"UPDATE player_aliases SET player_key={ph} WHERE player_key={ph}", (keep_key, duplicate_key))
                cursor.execute(f"""
                    INSERT INTO player_aliases (alias_key, player_key) VALUES ({ph}, {ph})
                    ON CONFLICT (alias_key) DO UPDATE SET player_key=excluded.player_key
                """, (duplicate_key, keep_key))
                self.conn.commit()
            except Exception as e:
                print(f"Error linking players {duplicate_key} -> {keep_key}: {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                return False, "Could not link the players - check the server logs."
        for alias, target in list(self.player_aliases.items()):
            if target == duplicate_key:
                self.player_aliases[alias] = keep_key
        self.player_aliases[duplicate_key] = keep_key
        self._bios_cache = None
        return True, f"Linked {duplicate_key} to {merged['display_name']}."

    def unlink_player(self, alias_key):
        """Undo a link: the alias becomes its own (bio-less) player again."""
        if alias_key not in self.player_aliases:
            return False, "That entry isn't linked."
        ph = "?" if self.param_style == 'sqlite' else "%s"
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(f"DELETE FROM player_aliases WHERE alias_key={ph}", (alias_key,))
                self.conn.commit()
            except Exception as e:
                print(f"Error unlinking {alias_key}: {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                return False, "Could not unlink - check the server logs."
        self.player_aliases.pop(alias_key, None)
        self._bios_cache = None
        return True, f"Unlinked {alias_key}."

    def get_player_bio(self, player_key):
        """Return the saved bio dict for a player key (following links), or None."""
        player_key = self.resolve_player_key(player_key)
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

    def get_all_player_bios(self, include_aliases=True):
        """
        Every saved bio keyed by player_key (cached until a bio is saved). Linked
        duplicate keys map to the kept player's bio so lookups by any key work;
        pass include_aliases=False when listing or exporting players.
        """
        bios = self._load_player_bios()
        if not include_aliases or not self.player_aliases:
            return bios
        with_aliases = dict(bios)
        for alias in self.player_aliases:
            target = bios.get(self.resolve_player_key(alias))
            if target:
                with_aliases.setdefault(alias, target)
        return with_aliases

    def _load_player_bios(self):
        cached = getattr(self, "_bios_cache", None)
        if cached is not None:
            return cached
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT * FROM player_bios")
                cols = [d[0] for d in cursor.description]
                self._bios_cache = {row[cols.index('player_key')]: dict(zip(cols, row)) for row in cursor.fetchall()}
                return self._bios_cache
            except Exception as e:
                print(f"Error reading player bios: {e}")
                return {}

    def save_player_bio(self, player_key, fields):
        self._bios_cache = None
        player_key = self.resolve_player_key(player_key)
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

            self.broadcast_matches(changed_match_ids)

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
                    if score_changed:
                        self.feed_score_ts[str(match_id)] = candidate_data['timestamp']
                    # (skipped while a commentator is manually scoring - their points are recorded instead)
                    if score_changed and not is_plan and str(match_id) not in self.manual_scores:
                        try:
                            self._insert_history(cursor, match_id, candidate_data['timestamp'], candidate_data)
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

    def get_latest_data(self, court_number=None, all_tournaments=False):
        """
        Retrieves all match data from the cache (cached in-process between writes)
        and filters set columns for output. Optionally filters by court number.
        Matches outside the /config tournament filter are dropped unless
        all_tournaments is set.
        """
        with self.db_lock:
            if self._latest_cache is None:
                self._latest_cache = self._load_all_matches()
            matches_list = self._latest_cache

        # Matches staged from the LTA order of play sit alongside the feed until TennisTicker has them
        staged, covered = staged_match_rows(matches_list)
        if staged:
            # TennisTicker's planned copy of a staged match is hidden until it goes live
            matches_list = [m for m in matches_list if str(m.get('matchid')) not in covered] + staged

        # Matches linked to the LTA order of play take LTA's names, event and final result
        matches_list = apply_lta_preference(matches_list)

        if self.manual_scores:
            # A commentator's manual score replaces the feed's score for that match everywhere,
            # unless TennisTicker has changed the score since the commentator's last action.
            manual = dict(self.manual_scores)
            matches_list = [
                manual_scoring.apply_to_match(m, manual[str(m.get('matchid'))], MAX_SETS)
                if str(m.get('matchid')) in manual and not self.feed_is_newer(m.get('matchid')) else m
                for m in matches_list
            ]

        if not all_tournaments:
            matches_list = filter_match_tournaments(matches_list)

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
    global CURRENT_TOURNAMENT_ID, TT_USERID, TT_CONTRACT, MATCH_TOURNID_FILTER, RESULT_HOLD_MINUTES

    with state_lock:
        saved_hold = mgr.get_setting("result_hold_minutes")
        if saved_hold is not None and str(saved_hold).isdigit():
            RESULT_HOLD_MINUTES = int(saved_hold)
        saved_filter = mgr.get_setting("match_tournid_filter")
        if saved_filter is not None:
            MATCH_TOURNID_FILTER = [t for t in saved_filter.split(",") if t]
        saved_tournid = mgr.get_setting("tournament_id")
        if saved_tournid and saved_tournid.isdigit():
            CURRENT_TOURNAMENT_ID = saved_tournid
        saved_userid = mgr.get_setting("tt_userid")
        if saved_userid:
            TT_USERID = saved_userid
        saved_contract = mgr.get_setting("tt_contract")
        if saved_contract:
            TT_CONTRACT = saved_contract

    load_lta_schedule(mgr)
    print(f"Settings loaded: tournament={CURRENT_TOURNAMENT_ID}, userid={TT_USERID}, contract={TT_CONTRACT}, "
          f"match filter={MATCH_TOURNID_FILTER or 'all'}")


def continuous_scraper_loop():
    """The main loop that runs in a separate thread to continuously scrape the XML."""
    global manager
    manager = XMLCacheManager(DB_NAME)
    load_persisted_settings(manager)
    print("\n--- Scraper Loop Starting ---")

    # Schedule archiving and cleanup at midnight
    schedule.every().day.at("00:00").do(manager.archive_completed_previous_day_matches)
    schedule.every().day.at("00:05").do(manager.cleanup_old_archived_matches)
    schedule.every(LTA_SCHEDULE_REFRESH_MIN).minutes.do(lta_schedule_tick)
    lta_schedule_tick()

    while True:
        # Run scheduled tasks
        schedule.run_pending()

        xml_data = manager.fetch_xml_data()
        if xml_data:
            manager.parse_and_cache_data(xml_data)
            try:
                link_lta_schedule()
            except Exception as e:
                print(f"Error linking LTA schedule: {e}")

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
# Country/county suffix: "(GBR)", "(S.W)", "(H&W)", or empty "()" as the feed sometimes sends
PLAYER_COUNTRY_RE = re.compile(r'\(((?:[A-Za-z.&]{2,4})?)\)')


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


def filter_match_tournaments(matches):
    """Keep only matches whose own tournid is in the /config filter (all when unset)."""
    wanted = set(MATCH_TOURNID_FILTER)
    if not wanted:
        return matches
    return [m for m in matches if m.get('staged') or str(m.get('tournid') or '').strip() in wanted]


def feed_tournaments():
    """Tournaments present in the cached feed: [{"tournid", "tname", "count"}], plus filtered ids not seen."""
    found = {}
    if manager:
        for m in manager.get_latest_data(all_tournaments=True):
            tid = str(m.get('tournid') or '').strip()
            if not tid:
                continue
            entry = found.setdefault(tid, {"tournid": tid, "tname": str(m.get('tname') or '').strip(), "count": 0})
            entry["count"] += 1
    for tid in MATCH_TOURNID_FILTER:
        found.setdefault(tid, {"tournid": tid, "tname": "(no matches in feed yet)", "count": 0})
    return sorted(found.values(), key=lambda t: (-t["count"], t["tournid"]))


def update_match_tournid_filter(tournids):
    """Save the match tournid filter and push the re-filtered list to open dashboards."""
    global MATCH_TOURNID_FILTER
    clean = sorted({t.strip() for t in tournids if t and t.strip()})
    if any(not t.isdigit() for t in clean):
        return False, "Tournament IDs must be numbers."
    with state_lock:
        MATCH_TOURNID_FILTER = clean
    if manager:
        manager.save_setting("match_tournid_filter", ",".join(clean))
        socketio.emit('live_updates', {
            "timestamp": datetime.now().strftime('%H:%M:%S'),
            "live_matches": manager.get_latest_data()
        }, to='dashboard')
    return True, ("Showing matches from tournament ID " + ", ".join(clean)) if clean else "Showing matches from all tournaments."


def all_known_matches():
    """Current cached matches plus archived ones, de-duplicated by matchid (staged pre-loads excluded)."""
    if manager is None:
        return []
    # Staged rows are pre-loads from the LTA order of play, not results; their LTA-style
    # names would otherwise show up as extra players alongside the feed's names
    matches = [m for m in manager.get_latest_data() if not m.get('staged')]
    seen = {str(m.get('matchid')) for m in matches}
    for m in filter_match_tournaments(manager.get_archived_matches()):
        if str(m.get('matchid')) not in seen:
            matches.append(m)
    return matches


def _fmt_duration(sec):
    sec = int(sec or 0)
    h, m = sec // 3600, sec % 3600 // 60
    return f"{h}h {m:02d}m" if h else f"{m}m"


def player_event_stats(player_key):
    """
    This-event statistics for one player from our own feed data (cache + archive):
    sets/games, tiebreaks, deciding sets, comebacks, bagels, time on court, plus
    serve stats from any matches scored courtside on /score.
    """
    player_key = manager.resolve_player_key(player_key) if manager else player_key
    keys = {player_key} | set(manager.aliases_of(player_key) if manager else [])
    s = {"matches": 0, "wins": 0, "losses": 0, "sets_won": 0, "sets_lost": 0, "games_won": 0, "games_lost": 0,
         "tiebreaks_won": 0, "tiebreaks_lost": 0, "match_tiebreaks_won": 0, "match_tiebreaks_lost": 0,
         "deciding_won": 0, "deciding_lost": 0, "straight_sets_wins": 0, "comebacks": 0,
         "bagels_given": 0, "bagels_received": 0, "time_on_court_sec": 0, "longest_match_sec": 0, "timed_matches": 0}
    serve = {"aces": 0, "double_faults": 0, "serve_points": 0, "first_serve_in": 0,
             "break_points_won": 0, "break_points": 0, "matches": 0}
    wins_detail = []   # {"score": "6-1 6-0", "conceded": 1, "opponent": ..., "sets": 2}
    partners = {}      # partner name -> [wins, losses]

    for m in all_known_matches():
        side1 = side_player_entries(m.get('player1_full') or m.get('player1'))
        side2 = side_player_entries(m.get('player2_full') or m.get('player2'))
        on1 = any(p['key'] in keys for p in side1)
        on2 = any(p['key'] in keys for p in side2)
        if not (on1 or on2):
            continue
        mine_idx = 1 if on1 else 2

        # Serve stats from courtside scoring, if this match was scored on /score
        state = manager.manual_scores.get(str(m.get('matchid'))) if manager else None
        if state and state.get("log"):
            st = manual_scoring.stats(state).get(mine_idx) or {}
            for k in ("aces", "double_faults", "serve_points", "first_serve_in", "break_points_won", "break_points"):
                serve[k] += st.get(k, 0)
            serve["matches"] += 1

        winner_code = str(m.get('winner') or '').strip()
        if classify_match_status(m) != "COMPLETED" or winner_code not in ("1", "2"):
            continue
        won = winner_code == str(mine_idx)
        s["matches"] += 1
        s["wins" if won else "losses"] += 1
        own_side, opp_side = (side1, side2) if mine_idx == 1 else (side2, side1)
        bios_now = manager.get_all_player_bios() if manager else {}
        name_of = lambda p: (bios_now.get(p['key']) or {}).get('display_name') or _feed_full_name(p['name'])
        opponent = " / ".join(name_of(p) for p in opp_side) or "TBC"
        for partner in (p for p in own_side if p['key'] not in keys):
            rec = partners.setdefault(name_of(partner), [0, 0])
            rec[0 if won else 1] += 1
        line, conceded = [], 0

        set_results = []
        for i in range(1, min(int(m.get('sets_played_count') or 0), MAX_SETS) + 1):
            try:
                a, b = int(m.get(f'set{i}_p1') or 0), int(m.get(f'set{i}_p2') or 0)
            except (TypeError, ValueError):
                continue
            if a == 0 and b == 0:
                continue
            mine, theirs = (a, b) if mine_idx == 1 else (b, a)
            line.append(f"{mine}-{theirs}")
            if max(mine, theirs) < 10:
                conceded += theirs
            set_won = mine > theirs
            set_results.append(set_won)
            s["sets_won" if set_won else "sets_lost"] += 1
            if max(mine, theirs) >= 10:            # match tiebreak played as the deciding "set"
                s["match_tiebreaks_won" if set_won else "match_tiebreaks_lost"] += 1
                continue
            s["games_won"] += mine
            s["games_lost"] += theirs
            if str(m.get(f'set{i}_tb') or '').strip() or {mine, theirs} == {7, 6}:
                s["tiebreaks_won" if set_won else "tiebreaks_lost"] += 1
            if (mine, theirs) == (6, 0):
                s["bagels_given"] += 1
            elif (mine, theirs) == (0, 6):
                s["bagels_received"] += 1

        if won and line:
            wins_detail.append({"score": " ".join(line), "conceded": conceded, "opponent": opponent,
                                "sets": len(line)})
        if set_results:
            lost_sets = set_results.count(False)
            if won and lost_sets == 0:
                s["straight_sets_wins"] += 1
            if won and not set_results[0]:
                s["comebacks"] += 1
            if lost_sets and set_results.count(True) and abs(set_results.count(True) - lost_sets) == 1 \
                    and len(set_results) >= 3:
                s["deciding_won" if won else "deciding_lost"] += 1

        history = manager.get_match_history(str(m.get('matchid'))) if manager else []
        if len(history) >= 2:
            dur = history[-1]["ts"] - history[0]["ts"]
            if 0 < dur < 6 * 3600:
                s["time_on_court_sec"] += dur
                s["longest_match_sec"] = max(s["longest_match_sec"], dur)
                s["timed_matches"] += 1

    games = s["games_won"] + s["games_lost"]
    s["games_pct"] = round(100 * s["games_won"] / games) if games else None
    s["avg_match_sec"] = s["time_on_court_sec"] // s["timed_matches"] if s["timed_matches"] else 0
    s["wins_detail"] = wins_detail
    s["partners"] = {name: {"wins": w, "losses": l} for name, (w, l) in partners.items()}
    if serve["matches"]:
        serve["first_serve_pct"] = round(100 * serve["first_serve_in"] / serve["serve_points"]) if serve["serve_points"] else None
        s["serve"] = serve
    return s


def _times(n):
    return {1: "once", 2: "twice", 3: "three times", 4: "four times"}.get(n, f"{n} times")


def standout_talking_points(s):
    """Eye-catching results this event: repeated scorelines, dominant wins, double bagels, partnerships."""
    points = []
    wins = s.get("wins_detail") or []
    straight = [w for w in wins if w["sets"] >= 2]
    counts = {}
    for w in straight:
        counts[w["score"]] = counts.get(w["score"], 0) + 1
    repeated = sorted((c, sc) for sc, c in counts.items() if c >= 2)
    for c, sc in reversed(repeated):
        points.append(f"Won {sc} {_times(c)} this event.")
    for w in straight:
        if w["score"] == "6-0 6-0":
            points.append(f"Double-bagel win this event: 6-0 6-0 v {w['opponent']}.")
            break
    dominant = [w for w in straight if w["conceded"] <= 2]
    if len(dominant) >= 2 and not (repeated and len(repeated) == 1 and repeated[0][0] == len(dominant)):
        example = min(dominant, key=lambda w: w["conceded"])["score"]
        points.append(f"Has won {len(dominant)} matches dropping two games or fewer this event (e.g. {example}).")
    if straight and len(wins) >= 2:
        best = min(straight, key=lambda w: (w["conceded"], -w["sets"]))
        if best["conceded"] <= 4 and best["score"] != "6-0 6-0":
            points.append(f"Biggest win this event: {best['score']} v {best['opponent']}.")
    for partner, rec in (s.get("partners") or {}).items():
        if rec["wins"] + rec["losses"] >= 2:
            if rec["losses"] == 0:
                points.append(f"Unbeaten with {partner} this event ({rec['wins']}-0).")
            else:
                points.append(f"{rec['wins']}-{rec['losses']} with {partner} this event.")
    return points


def event_talking_points(s, name=""):
    """Commentator sentences from this-event stats (most newsworthy first)."""
    first = (name or "").split(" ")[0] or "They"
    points = standout_talking_points(s)
    if s["matches"] >= 1 and s["losses"] == 0 and s["sets_lost"] == 0 and s["wins"]:
        points.append(f"Yet to drop a set this event ({s['wins']} win{'s' if s['wins'] != 1 else ''}).")
    if s["wins"] and s["games_lost"] <= 4 * s["matches"] and s["games_won"] + s["games_lost"] >= 12:
        points.append(f"Has dropped only {s['games_lost']} games in {s['matches']} match{'es' if s['matches'] != 1 else ''} this event.")
    elif s["games_pct"] is not None and s["matches"]:
        points.append(f"Won {s['games_pct']}% of games this event ({s['games_won']}-{s['games_lost']}).")
    if s["comebacks"]:
        points.append(f"Has come from a set down to win {s['comebacks']} time{'s' if s['comebacks'] != 1 else ''} this event.")
    deciders = s["deciding_won"] + s["deciding_lost"]
    mtb = s["match_tiebreaks_won"] + s["match_tiebreaks_lost"]
    if deciders and deciders != mtb:   # padel deciders are match tiebreaks: reported below
        points.append(f"{s['deciding_won']}-{s['deciding_lost']} in deciding sets this event.")
    if mtb:
        points.append(f"{s['match_tiebreaks_won']}-{s['match_tiebreaks_lost']} in match tiebreaks this event.")
    tbs = s["tiebreaks_won"] + s["tiebreaks_lost"]
    if tbs:
        points.append(f"{s['tiebreaks_won']}-{s['tiebreaks_lost']} in tiebreaks this event.")
    if s["bagels_given"]:
        points.append(f"Handed out {s['bagels_given']} bagel{'s' if s['bagels_given'] != 1 else ''} (6-0 set) this event.")
    if s["timed_matches"]:
        points.append(f"{first} has spent {_fmt_duration(s['time_on_court_sec'])} on court this event"
                      + (f"; longest match {_fmt_duration(s['longest_match_sec'])}." if s["timed_matches"] > 1 else "."))
    sv = s.get("serve")
    if sv and sv["serve_points"] >= 10:
        bits = [f"{sv['aces']} ace{'s' if sv['aces'] != 1 else ''}", f"{sv['double_faults']} double fault{'s' if sv['double_faults'] != 1 else ''}"]
        if sv.get("first_serve_pct") is not None:
            bits.append(f"{sv['first_serve_pct']}% first serves in")
        if sv["break_points"]:
            bits.append(f"{sv['break_points_won']}/{sv['break_points']} break points converted")
        points.append("Courtside stats: " + ", ".join(bits) + ".")
    return points


def compute_player_record(player_key):
    """
    Tournament W/L and per-match results for one player, computed from our
    own cached + archived feed data (no external sources).
    """
    wins = 0
    losses = 0
    results = []

    bios = manager.get_all_player_bios() if manager else {}
    player_key = manager.resolve_player_key(player_key) if manager else player_key
    keys = {player_key} | set(manager.aliases_of(player_key) if manager else [])

    def display(p):
        bio = bios.get(p['key'])
        return bio['display_name'] if bio and bio.get('display_name') else p['name']

    for m in all_known_matches():
        side1 = side_player_entries(m.get('player1_full') or m.get('player1'))
        side2 = side_player_entries(m.get('player2_full') or m.get('player2'))
        on1 = any(p['key'] in keys for p in side1)
        on2 = any(p['key'] in keys for p in side2)
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
            "partner": " / ".join(display(p) for p in own_side if p['key'] not in keys),
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
                key = manager.resolve_player_key(p['key']) if manager else p['key']
                existing = players.get(key)
                if not existing:
                    players[key] = {"name": p['name'], "country": p['country'], "has_bio": False}
                elif not existing['country'] and p['country']:
                    existing['country'] = p['country']

    if manager:
        for key, bio in manager.get_all_player_bios(include_aliases=False).items():
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

    # Prefer an exact court-name match ("LTA-OC-2"); fall back to the numeric
    # token ("2") for callers that only pass a court number.
    court_token = str(court or '').strip().lower()
    court_number = normalize_court_number(court)

    court_matches = []
    if court_token:
        court_matches = [
            x for x in all_matches
            if str(x.get("court") or '').strip().lower() == court_token
        ]
    if not court_matches and court_number:
        court_matches = [
            x for x in all_matches
            if normalize_court_number(x.get("court")) == court_number
        ]

    if court_token or court_number:
        if court_matches:
            # Same rule as the vMix feed, so overlays and vMix always agree
            return pick_court_match(court_matches)

    # 1) Explicit matchid (a staged match's id follows it once TennisTicker carries it)
    if matchid:
        m = next((x for x in all_matches if str(x.get("matchid")) == str(matchid)), None)
        if m is None:
            alias = staged_match_alias(matchid)
            m = next((x for x in all_matches if str(x.get("matchid")) == alias), None) if alias else None
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
            # A saved blank means "No court assigned" - it must not fall back to the default court
            if f"stream_court_{idx + 1}" in settings:
                stream_courts[idx] = (settings.get(f"stream_court_{idx + 1}") or "").strip()
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


def pinned_courts_for_display():
    """
    Courts listed first on Commentary / Schedule / API Links: the courts of the live
    stream panels in use, resolved to real court names ("1" -> "LTA-OC-1", "LTA-IC-1").
    Pins with no matching court are ignored; Config can switch pinning off.
    """
    if manager is None or (manager.get_setting("pin_stream_courts") or "1") == "0":
        return []
    config = get_live_stream_config()
    numbers = [c for c in config["stream_courts"][:config["stream_count"]] if c]
    courts = sorted({str(m.get("court") or "").strip() for m in manager.get_latest_data() if m.get("court")},
                    key=court_sort_key)
    pinned = []
    for n in numbers:
        for c in courts:
            if (c == n or normalize_court_number(c) == n) and c not in pinned:
                pinned.append(c)
    return pinned


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


# ====================================================================
# Bug overlay appearance (edited via /caspar/bug/editor)
# ====================================================================

BUG_STYLE_SETTING_KEY = "bug_style"
BUG_CORNERS = ("top-left", "top-right", "bottom-left", "bottom-right")
HEX_COLOR_PATTERN = re.compile(r'^#[0-9A-Fa-f]{6}$')

DEFAULT_BUG_STYLE = {
    "corner": "bottom-left",
    "offset_x": 28,
    "offset_y": 32,
    "row_bg": "#f2f2f2",        # score row background (sampled from broadcast reference)
    "row_opacity": 100,         # percent
    "text_color": "#051b4a",    # player names / set scores (LTA navy)
    "accent_color": "#051b4a",  # logo panel + current-game box background
    "game_text_color": "#ffffff",
    "set_win_color": "#3ddc84",
    "show_game_box": True,      # untick for matches without live point scoring
}


def _hex_to_rgb(hex_color, fallback=(0, 0, 0)):
    h = str(hex_color or '').lstrip('#')
    if len(h) != 6:
        return fallback
    try:
        return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
    except ValueError:
        return fallback


def get_bug_style():
    """Bug appearance settings merged over defaults, plus derived CSS values."""
    style = dict(DEFAULT_BUG_STYLE)
    if manager:
        raw = manager.get_setting(BUG_STYLE_SETTING_KEY)
        if raw:
            try:
                saved = json.loads(raw)
                style.update({k: v for k, v in saved.items() if k in DEFAULT_BUG_STYLE})
            except (ValueError, TypeError) as e:
                print(f"Ignoring invalid saved bug style: {e}")

    # Coercion / clamping
    try:
        style['offset_x'] = max(0, min(1800, int(style['offset_x'])))
        style['offset_y'] = max(0, min(1000, int(style['offset_y'])))
        style['row_opacity'] = max(0, min(100, int(style['row_opacity'])))
    except (TypeError, ValueError):
        style['offset_x'], style['offset_y'], style['row_opacity'] = 60, 60, 90
    if style['corner'] not in BUG_CORNERS:
        style['corner'] = "top-left"
    for key in ("row_bg", "text_color", "accent_color", "game_text_color", "set_win_color"):
        if not HEX_COLOR_PATTERN.match(str(style[key])):
            style[key] = DEFAULT_BUG_STYLE[key]
    style['show_game_box'] = bool(style['show_game_box'])

    # Derived CSS
    r, g, b = _hex_to_rgb(style['row_bg'], (238, 241, 246))
    style['row_bg_rgba'] = f"rgba({r},{g},{b},{style['row_opacity'] / 100:.2f})"
    sr, sg, sb = _hex_to_rgb(style['set_win_color'], (61, 220, 132))
    style['set_win_bg_rgba'] = f"rgba({sr},{sg},{sb},0.25)"
    style['set_win_border_rgba'] = f"rgba({sr},{sg},{sb},0.9)"

    x, y = style['offset_x'], style['offset_y']
    style['position_css'] = {
        "top-left": f"top:{y}px;left:{x}px;right:auto;bottom:auto;",
        "top-right": f"top:{y}px;right:{x}px;left:auto;bottom:auto;",
        "bottom-left": f"bottom:{y}px;left:{x}px;right:auto;top:auto;",
        "bottom-right": f"bottom:{y}px;right:{x}px;left:auto;top:auto;",
    }[style['corner']]
    return style


def save_bug_style_from_form(form):
    """Validate and persist bug appearance settings from the editor page."""
    if not manager:
        return False, "Bug style can only be saved while the scraper manager is running."

    style = {}
    corner = (form.get('corner') or '').strip()
    if corner not in BUG_CORNERS:
        return False, "Invalid corner selection."
    style['corner'] = corner

    for key, limit in (("offset_x", 1800), ("offset_y", 1000), ("row_opacity", 100)):
        raw = (form.get(key) or '').strip()
        if not raw.lstrip('-').isdigit():
            return False, f"{key.replace('_', ' ').title()} must be a number."
        style[key] = max(0, min(limit, int(raw)))

    for key in ("row_bg", "text_color", "accent_color", "game_text_color", "set_win_color"):
        raw = (form.get(key) or '').strip()
        if not HEX_COLOR_PATTERN.match(raw):
            return False, f"{key.replace('_', ' ').title()} must be a hex colour like #0e1f4d."
        style[key] = raw

    style['show_game_box'] = bool(form.get('show_game_box'))

    manager.save_setting(BUG_STYLE_SETTING_KEY, json.dumps(style))
    return True, "Bug style saved - overlays pick it up on their next (re)load."


BUG_LOGO_MEDIA_NAME = "bug_logo"
BUG_LOGO_MAX_BYTES = 5 * 1024 * 1024
IMAGE_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)


def _image_type(payload):
    """MIME type from the file's first bytes (not its name), or None if it isn't a supported image."""
    for signature, mime in IMAGE_SIGNATURES:
        if payload.startswith(signature):
            return mime
    if payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image/webp"
    return None


def get_bug_logo_url():
    """Return the uploaded bug logo URL if one has been configured."""
    if not manager:
        return None

    version = manager.get_media_version(BUG_LOGO_MEDIA_NAME)
    if version:
        return url_for("media_file", name=BUG_LOGO_MEDIA_NAME, v=version)

    # Logos uploaded before they were stored in the database
    logo_filename = (manager.get_setting(BUG_LOGO_SETTING_KEY) or "").strip()
    if not logo_filename:
        return None

    logo_path = os.path.join(app.root_path, BUG_LOGO_UPLOAD_DIR, logo_filename)
    if not os.path.exists(logo_path):
        return None

    return url_for("static", filename=f"uploads/caspar_bug/{logo_filename}")


def save_bug_logo_upload(uploaded_file):
    """
    Persist an uploaded logo for the Caspar bug overlay. Stored in the database
    (not the app folder), so it works with read-only mounts and survives redeploys.
    """
    if not manager:
        return False, "Logo uploads require the scraper manager to be running."

    if uploaded_file is None or not uploaded_file.filename:
        return False, "Choose a logo image to upload."

    try:
        payload = uploaded_file.read(BUG_LOGO_MAX_BYTES + 1)
    except Exception as e:
        print(f"Error reading uploaded logo: {e}")
        return False, "Could not read the uploaded file - try again."

    if not payload:
        return False, "That file is empty."
    if len(payload) > BUG_LOGO_MAX_BYTES:
        return False, "Logo must be 5 MB or smaller."

    content_type = _image_type(payload)
    if not content_type:
        return False, "Logo must be a PNG, JPG, GIF or WEBP image."

    if not manager.save_media(BUG_LOGO_MEDIA_NAME, content_type, payload):
        return False, "Could not save the logo - check the server logs."
    return True, "Saved bug logo image."


@app.route('/media/<name>', methods=['GET'])
def media_file(name):
    """Serve an image stored in the database. Links carry ?v=<version>, so it can be cached hard."""
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503
    found = manager.get_media(name)
    if not found:
        return jsonify({"error": "Not found."}), 404
    content_type, payload, version = found
    response = make_response(payload)
    response.headers['Content-Type'] = content_type
    response.headers['Cache-Control'] = 'public, max-age=31536000, immutable' if request.args.get('v') \
        else 'no-cache'
    response.headers['ETag'] = f'"{name}-{version}"'
    return response

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
    response = make_response(render_template('api_links.html', pinned_courts=pinned_courts_for_display()))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/stats')
def stats_page():
    """Commentary screen: live match stats for commentators, optionally focused on one court."""
    court = (request.args.get('court') or '').strip()

    pinned_courts = pinned_courts_for_display()

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
    global RESULT_HOLD_MINUTES
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
        elif form_name == 'pin_courts':
            on = request.form.get('pin') == '1'
            if manager:
                manager.save_setting("pin_stream_courts", "1" if on else "0")
            message = ("Stream courts are listed first on Commentary, Schedule and API Links."
                       if on else "Courts are listed in normal order everywhere (no pinned courts).")
        elif form_name == 'result_hold':
            try:
                minutes = max(0, min(120, int(request.form.get('minutes', '5'))))
                RESULT_HOLD_MINUTES = minutes
                if manager:
                    manager.save_setting("result_hold_minutes", str(minutes))
                message = (f"Finished matches stay on air for {minutes} minute(s), or until the next match on the court goes live."
                           if minutes else "Courts move to their next match as soon as a match finishes.")
            except ValueError:
                error = "Enter a number of minutes."
        elif form_name == 'lta_schedule':
            ok, msg = refresh_lta_schedule(request.form.get('lta_url', '').strip() or None)
            if ok:
                message = msg
            else:
                error = msg
        elif form_name == 'lta_auto_stage':
            lta_stage_prefs["auto"] = request.form.get('auto') == '1'
            if request.form.get('reset_skipped') == '1':
                lta_stage_prefs["skipped"] = []
            save_lta_schedule()
            notify_schedule_changed()
            message = ("Auto-staging on: today's matches with a court are ready to score."
                       if lta_stage_prefs["auto"] else "Auto-staging off: only matches you stage are pre-loaded.")
        elif form_name == 'lta_court_map':
            manual = {}
            for lta_court, tt_court in zip(request.form.getlist('lta_court'), request.form.getlist('tt_court')):
                if lta_court.strip() and tt_court.strip():
                    manual[lta_court.strip()] = tt_court.strip()
            lta_court_map["manual"] = manual
            save_lta_schedule()
            message = f"Saved {len(manual)} court mapping override(s)."
        elif form_name == 'match_tournid_filter':
            # The feed tournament ID itself is only changeable on /admin
            picked = request.form.getlist('match_tournid')
            picked += re.split(r'[\s,]+', request.form.get('match_tournid_extra', ''))
            ok, msg = update_match_tournid_filter(picked)
            if ok:
                message = msg
            else:
                error = msg

    with state_lock:
        tour_id = CURRENT_TOURNAMENT_ID
        interval = SCRAPE_INTERVAL
        match_filter = list(MATCH_TOURNID_FILTER)

    stream_config = get_live_stream_config()
    available_courts = get_available_court_numbers()
    bug_logo_url = get_bug_logo_url()

    response = make_response(render_template(
        'config.html',
        tournament_id=tour_id,
        match_tournid_filter=match_filter,
        result_hold_minutes=RESULT_HOLD_MINUTES,
        pin_stream_courts=(manager.get_setting("pin_stream_courts") or "1") != "0" if manager else True,
        pinned_now=pinned_courts_for_display(),
        feed_tournaments=feed_tournaments(),
        lta_schedule_info={
            "url": lta_schedule.get("url") or "",
            "count": len(lta_schedule.get("matches") or []),
            "linked": sum(1 for m in lta_schedule.get("matches") or [] if m.get("tt_matchid")),
            "fetched": datetime.fromtimestamp(lta_schedule["fetched_at"]).strftime('%Y-%m-%d %H:%M')
            if lta_schedule.get("fetched_at") else "",
            "error": lta_schedule.get("error") or "",
            "refresh_min": LTA_SCHEDULE_REFRESH_MIN,
            "auto_stage": lta_stage_prefs.get("auto", True),
            "auto_count": sum(1 for s in effective_staged().values() if s.get("auto")),
            "skipped_count": len(lta_stage_prefs.get("skipped") or []),
        },
        lta_courts=[
            {"lta": c, "learned": lta_court_map["learned"].get(c, ""), "manual": lta_court_map["manual"].get(c, "")}
            for c in sorted({m["lta_court"] for m in lta_schedule.get("matches") or [] if m.get("lta_court")})
        ],
        tt_courts=sorted({str(m.get("court")) for m in (manager.get_latest_data(all_tournaments=True) if manager else [])
                          if m.get("court")}, key=court_sort_key),
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
    """API endpoint to change the CURRENT_TOURNAMENT_ID that the scraper tracks (admin session only)."""
    if not session.get("admin_authenticated"):
        return jsonify({"error": "Admin login required - change the feed tournament ID on /admin."}), 403
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
# Manual scoring: a commentator scores the match they are watching
# ====================================================================
manual_score_lock = Lock()  # serialises read-modify-write of a match's scoring state


def manual_score_response(match_id):
    state = manager.manual_scores.get(str(match_id))
    match = next((m for m in manager.get_latest_data(all_tournaments=True)
                  if str(m.get('matchid')) == str(match_id)), None)
    if match is None:
        match = manager.get_feed_match(match_id)
    bios = manager.get_all_player_bios()
    return {
        "status": "success",
        "matchid": str(match_id),
        "names": [full_side_name((match or {}).get("player1_full") or (match or {}).get("player1"), bios),
                  full_side_name((match or {}).get("player2_full") or (match or {}).get("player2"), bios)],
        "active": state is not None,
        "state": manual_scoring.summary(state) if state else None,
        # The feed changed the score after the last manual action, so the feed's score is on air
        "feed_newer": manager.feed_is_newer(match_id),
        "feed_lock": bool(state and state.get("feed_lock")),
        "match": match,
    }


@app.route('/api/v1/manual/<match_id>', methods=['GET', 'POST'])
def api_manual_score(match_id):
    """
    GET: current manual scoring state. POST JSON {"action": ...}:
      start   {from_feed, preset, rules, server: 1|2, status: warmup|live}  begin scoring this match
      point   {side: 1|2, kind}       award a point; kind: normal, winner, forced_error, unforced_error
      game    {side: 1|2}             award the game (game-by-game scoring, no point detail)
      ace                             point to the server
      fault                           1st serve fault -> 2nd serve; on 2nd serve = double fault
      penalty {side: 1|2}             point penalty awarded to side
      undo                            step back one action
      server  {side: 1|2}             set who is serving
      games   {side: 1|2, delta: ±1}  correct the current set's games
      rules   {preset | rules}        change format
      status  {status: warmup|live|suspended}
      end     {side: 1|2, reason: retired|walkover|default}  finish early; side = winner
      lock    {locked: true|false}    keep this score on air even when the feed changes
      complete                        mark a finished match complete (until then its result stays on air)
      resync                          carry on from the feed's current score
      stop                            hand the match back to the feed
    Whichever source changed the score most recently is shown (unless locked); scoring
    while the feed is ahead first catches up from the feed's score, keeping the stats log.
    """
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503
    match_id = str(match_id)
    # A staged match that TennisTicker now carries is scored under the TennisTicker id
    if match_id.startswith(STAGED_PREFIX) and manager.get_feed_match(match_id) is None:
        match_id = staged_match_alias(match_id) or match_id
    if request.method == 'GET':
        return jsonify(manual_score_response(match_id))

    body = request.get_json(silent=True) or {}
    action = str(body.get('action') or '').lower()
    try:
        side = int(body.get('side') or 0)
    except (TypeError, ValueError):
        side = 0
    scoring_actions = ('point', 'ace', 'fault', 'penalty', 'games', 'game')

    with manual_score_lock:
        state = manager.manual_scores.get(match_id)
        feed_match = manager.get_feed_match(match_id)   # the feed's own row, no manual overlay
        if feed_match is None:
            return jsonify({"error": f"Match {match_id} is not in the feed."}), 404

        if action == 'stop':
            if state is not None:
                manager.save_manual_score(match_id, state, active=False)
                manager.broadcast_matches([match_id])
            return jsonify(manual_score_response(match_id))

        if action == 'start':
            if state is None:
                padel = 'padel' in f"{feed_match.get('tname') or ''} {feed_match.get('matchname') or ''}".lower()
                rules = (manual_scoring.rules_for_preset(body['preset']) if body.get('preset') in manual_scoring.PRESETS
                         else manual_scoring.default_rules(padel))
                rules = manual_scoring.set_rules(manual_scoring.new_state(rules), body.get('rules') or {})["rules"]
                from_feed = body.get('from_feed', True) and feed_match.get('sets_played_count')
                state = (manual_scoring.state_from_match(feed_match, rules, MAX_SETS) if from_feed
                         else manual_scoring.new_state(rules, server=side if side in (1, 2) else 1))
                if body.get('status') in ('warmup', 'live'):
                    state["status"] = body['status']
                state["feed_lock"] = bool(body.get('lock'))
                state["preset"] = body.get('preset') if body.get('preset') in manual_scoring.PRESETS else ''
        elif state is None:
            return jsonify({"error": "Manual scoring is not active for this match - start it first."}), 409
        else:
            if action == 'resync' or (action in scoring_actions and manager.feed_is_newer(match_id)):
                # TennisTicker has moved on since the last manual action: carry on from its score
                fresh = manual_scoring.state_from_match(feed_match, state["rules"], MAX_SETS)
                for key in ("log", "started_at", "feed_lock"):
                    if key in state:
                        fresh[key] = state[key]
                state = fresh
            if action == 'point':
                manual_scoring.point(state, side, str(body.get('kind') or 'normal'))
            elif action == 'game':
                manual_scoring.award_game(state, side)
            elif action == 'ace':
                manual_scoring.ace(state)
            elif action == 'fault':
                manual_scoring.fault(state)
            elif action == 'penalty':
                manual_scoring.penalty(state, side)
            elif action == 'undo':
                manual_scoring.undo(state)
            elif action == 'server':
                manual_scoring.set_server(state, side)
            elif action == 'games':
                manual_scoring.adjust_games(state, side, 1 if int(body.get('delta') or 1) > 0 else -1)
            elif action == 'rules':
                rules = (manual_scoring.rules_for_preset(body['preset']) if body.get('preset') in manual_scoring.PRESETS
                         else body.get('rules') or {})
                manual_scoring.set_rules(state, rules)
                state["preset"] = body.get('preset') if body.get('preset') in manual_scoring.PRESETS else ''
            elif action == 'status':
                manual_scoring.set_status(state, str(body.get('status') or ''))
            elif action == 'end':
                manual_scoring.end_match(state, side, str(body.get('reason') or ''))
            elif action == 'lock':
                state["feed_lock"] = bool(body.get('locked'))
            elif action == 'complete':
                manual_scoring.confirm_result(state)
            elif action != 'resync':
                return jsonify({"error": f"Unknown action '{action}'."}), 400

        state["updated_at"] = time.time()
        manager.save_manual_score(match_id, state, active=True)
        if action in ('start', 'undo', 'resync', 'end') + scoring_actions:
            manager.record_history(manual_scoring.apply_to_match(feed_match, state, MAX_SETS))

    manager.broadcast_matches([match_id])
    return jsonify(manual_score_response(match_id))


@app.route('/api/v1/manual/presets', methods=['GET'])
def api_manual_presets():
    """Scoring formats offered on the scoring page."""
    return jsonify({k: {"label": v["label"], "rules": manual_scoring.rules_for_preset(k)}
                    for k, v in manual_scoring.PRESETS.items()})


@app.route('/score.webmanifest')
def score_manifest():
    """Web app manifest: 'Add to Home Screen' installs the scoring page as a full-screen app."""
    manifest = {
        "name": "Courtside Scoring",
        "short_name": "Scoring",
        "description": "Courtside tennis and padel scoring",
        "start_url": "/score",
        "scope": "/",
        "display": "fullscreen",
        "display_override": ["fullscreen", "standalone"],
        "orientation": "any",
        "background_color": "#0b0b11",
        "theme_color": "#0b0b11",
        "icons": [
            {"src": url_for('static', filename='icons/score-192.png'), "sizes": "192x192", "type": "image/png"},
            {"src": url_for('static', filename='icons/score-512.png'), "sizes": "512x512", "type": "image/png"},
            {"src": url_for('static', filename='icons/score-512.png'), "sizes": "512x512", "type": "image/png",
             "purpose": "maskable"},
        ],
    }
    response = make_response(json.dumps(manifest))
    response.headers['Content-Type'] = 'application/manifest+json'
    return response


@app.route('/score')
def score_page():
    """Courtside scoring (phone / iPad): pick a match, set it live, score every point."""
    response = make_response(render_template(
        'score.html',
        match_id=(request.args.get('match') or '').strip(),
        presets={k: v["label"] for k, v in manual_scoring.PRESETS.items()},
    ))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


# ====================================================================
# LTA import: pull profile links + spotter facts for a tournament's entrants
# ====================================================================
LTA_BASE = "https://competitions.lta.org.uk"
LTA_TOURNAMENT_RE = re.compile(r'tournament(?:\.aspx\?id=|/)([0-9A-Fa-f-]{36})')
LTA_HEADERS = {"User-Agent": "Mozilla/5.0 (TennisTickerGFX player import)"}
LTA_IMPORT_WORKERS = 4

# Shared progress for the background import (one run at a time)
lta_import_status = {"running": False, "done": 0, "total": 0, "message": "", "error": ""}


def _lta_text(fragment):
    """Strip tags/entities and collapse whitespace."""
    from html import unescape
    return re.sub(r'\s+', ' ', unescape(re.sub(r'<[^>]+>', ' ', fragment or ''))).strip()


def lta_session():
    """requests session that has accepted the LTA cookie wall."""
    session = requests.Session()
    session.headers.update(LTA_HEADERS)
    session.post(f"{LTA_BASE}/cookiewall/Save", data={
        "ReturnUrl": "/", "SettingsOpen": "false",
        "CookiePurposes": ["1", "2", "4", "8", "16"]
    }, allow_redirects=False, timeout=20)
    return session


def lta_tournament_players(session, tournament_id):
    """[{"player_no", "surname", "first", "country"}] for every entrant."""
    resp = session.post(
        f"{LTA_BASE}/tournament/{tournament_id}/Players/GetPlayersContent",
        headers={"X-Requested-With": "XMLHttpRequest"}, timeout=30)
    resp.raise_for_status()
    players = {}
    for no, name, country in re.findall(
            r'player\.aspx\?id=[^&]+&amp;player=(\d+)"[^>]*><span class="nav-link__value">([^<]+)</span></a>'
            r'\s*</h5>\s*(?:<div[^>]*>\s*<small[^>]*>\s*<span class="nav-link"><span class="nav-link__value">([^<]*)</span>)?',
            resp.text):
        surname, _, first = _lta_text(name).partition(',')
        players.setdefault(no, {"player_no": no, "surname": surname.strip(),
                                "first": first.strip(), "country": _lta_text(country)})
    return list(players.values())


def _lta_titles(fragment):
    """Titles/Finals markup -> [{"year", "result", "tournament", "event"}], newest year first."""
    titles, current_year = [], ""
    for year, medal, tname, ename in re.findall(
            r'list__label--loud">(\d{4})</dt>|title="(Winner|Finalist)".*?nav-link__value">([^<]+)</span>'
            r'.*?text--muted">.*?nav-link__value">([^<]+)</span>', fragment, re.S):
        if year:
            current_year = year
            continue
        titles.append({"year": current_year, "result": medal,
                       "tournament": _lta_text(tname), "event": _lta_text(ename)})
    return titles


def _lta_record(text):
    """'396 / 175 (571)' -> [396, 175], or None."""
    m = re.match(r'\s*(\d+)\s*/\s*(\d+)', text or '')
    return [int(m.group(1)), int(m.group(2))] if m else None


def lta_player_details(session, tournament_id, player_no):
    """
    Scrape the tournament player page + global profile into bio-ready facts.
    "stats" is the structured part stored as JSON in player_bios.lta_stats.
    """
    page = session.get(f"{LTA_BASE}/sport/player.aspx",
                       params={"id": tournament_id, "player": player_no}, timeout=30).text
    events = [_lta_text(e) for e in re.findall(
        r'event\.aspx\?id=[^"]*" class="nav-link text--link-white"><span class="nav-link__value">([^<]+)', page)]
    guid = re.search(r'/player-profile/([0-9A-Fa-f-]{36})', page)
    stats = {"member_no": "", "year_of_birth": "", "county": "", "wtn": {}, "records": {},
             "form": [], "titles": [], "events": events, "scraped": datetime.now().strftime("%Y-%m-%d")}
    details = {"lta_url": "", "full_name": "", "stats": stats}
    if not guid:
        return details

    details["lta_url"] = f"{LTA_BASE}/player-profile/{guid.group(1).lower()}"
    prof = session.get(details["lta_url"], timeout=30).text

    head = re.search(r'media__title--large">(.*?)</h2>', prof, re.S)
    if head:
        name = re.search(r'nav-link__value">([^<]+)', head.group(1))
        member = re.search(r'media__title-aside">\((\d+)\)', head.group(1))
        details["full_name"] = _lta_text(name.group(1)) if name else ""
        stats["member_no"] = member.group(1) if member else ""
    yob = re.search(r'Year of Birth:\s*(\d{4})', prof)
    stats["year_of_birth"] = yob.group(1) if yob else ""
    county = re.search(r'title="Play County".*?nav-link__value">([^<]+)', prof, re.S)
    stats["county"] = _lta_text(county.group(1)) if county else ""
    for kind, value in re.findall(
            r'tag-duo__title">(Singles|Doubles)</span>\s*<span class="tag-duo__value">(.*?)</span>', prof, re.S):
        stats["wtn"][kind.lower()] = _lta_text(value)

    # Win-loss per discipline: {"total": {"career": [w, l], "year": [w, l]}, "singles": …}
    for tab in ("Total", "Singles", "Doubles", "Mixed"):
        parts = prof.split(f'id="tabStats{tab}"', 1)
        if len(parts) < 2:
            continue
        block = parts[1].split('id="tabStats', 1)[0]
        record = {}
        for label, value in re.findall(
                r'list__label">(Career|This year)</dt>.*?list__value-start">(.*?)</span>', block, re.S):
            parsed = _lta_record(_lta_text(value))
            if parsed:
                record["career" if label == "Career" else "year"] = parsed
        if record:
            stats["records"][tab.lower()] = record
        if tab == "Total":
            # Recent results, oldest first
            stats["form"] = re.findall(r'match__status"[^>]*>([WL])<', block)

    # Full titles list (the profile itself only shows recent years)
    try:
        full = session.get(f"{details['lta_url']}/PersonHome/TitlesFinals",
                           headers={"X-Requested-With": "XMLHttpRequest"}, timeout=30).text
        stats["titles"] = _lta_titles(full)
    except Exception:
        stats["titles"] = []
    if not stats["titles"] and 'Titles/Finals' in prof:
        stats["titles"] = _lta_titles(prof.split('Titles/Finals', 1)[1])
    return details


def _pct(record):
    won, lost = record
    return round(100 * won / (won + lost)) if won + lost else 0


def _wl(record):
    return f"{record[0]}-{record[1]} ({_pct(record)}%)"


def lta_bio_fields(player, details):
    """Map scraped LTA facts onto player_bios columns."""
    stats = details["stats"]
    total = stats["records"].get("total", {})
    career_parts = []
    if stats["wtn"]:
        career_parts.append("WTN " + " / ".join(f"{k.title()} {v}" for k, v in stats["wtn"].items()))
    if total.get("career"):
        career_parts.append(f"Career W-L {_wl(total['career'])}")
    if total.get("year"):
        career_parts.append(f"{stats['scraped'][:4]} W-L {_wl(total['year'])}")
    wins = sum(1 for t in stats["titles"] if t["result"] == "Winner")
    if wins:
        career_parts.append(f"{wins} title(s)")

    notes_parts = []
    if stats["events"]:
        notes_parts.append("This event: " + "; ".join(stats["events"]))
    if stats["member_no"]:
        notes_parts.append(f"LTA no. {stats['member_no']}")

    # Profile names keep casing like "McGill" but are sometimes typed as
    # "aled smith" / "PAUL THOMAS"; the tournament list is consistently cased.
    full_name = details["full_name"]
    well_cased = full_name and not full_name.isupper() and all(w[:1].isupper() for w in full_name.split())
    first_last = f"{player['first']} {player['surname']}".strip()
    return {
        "display_name": full_name if well_cased else first_last,
        "country": player["country"],
        "born": stats["year_of_birth"],
        "hometown": f"{stats['county']} (county)" if stats["county"] else "",
        "career": " · ".join(career_parts),
        "notes": " · ".join(notes_parts),
        "lta_url": details["lta_url"],
        "lta_stats": json.dumps(stats, ensure_ascii=False),
    }


def parse_lta_stats(bio):
    """Structured LTA stats saved on a bio, or {}."""
    try:
        stats = json.loads((bio or {}).get('lta_stats') or '{}')
        return stats if isinstance(stats, dict) else {}
    except (TypeError, ValueError):
        return {}


def lta_talking_points(stats, name=""):
    """Commentator-ready sentences from structured LTA stats."""
    points = []      # performance lines, most newsworthy first
    background = []  # who/where lines, shown after
    first = (name or "").split(" ")[0] or "They"
    season = (stats.get("scraped") or str(datetime.now().year))[:4]
    records = stats.get("records") or {}
    total = records.get("total") or {}

    # Partner / events at this tournament
    for event in stats.get("events") or []:
        if " with " in event:
            ev, partner = event.split(" with ", 1)
            background.append(f"Partnering {partner} in {ev}.")

    yob = stats.get("year_of_birth")
    if yob and yob.isdigit():
        age = int(season) - int(yob)
        background.append(f"Born in {yob}, so {age} this year" +
                      (f" and playing out of {stats['county']}." if stats.get("county") else "."))
    elif stats.get("county"):
        background.append(f"Plays out of {stats['county']}.")

    career, year = total.get("career"), total.get("year")
    if career and sum(career) >= 5:
        line = f"Career record of {_wl(career)} across {sum(career)} LTA matches"
        if year and sum(year) >= 5:
            diff = _pct(year) - _pct(career)
            if diff >= 10:
                line += f" — and flying in {season} at {_wl(year)}"
            elif diff <= -10:
                line += f" — but a tougher {season} at {_wl(year)}"
            else:
                line += f"; {_wl(year)} in {season}"
        points.append(line + ".")

    # Specialism: one discipline making up most career matches
    split = {k: sum((records.get(k) or {}).get("career") or [0, 0]) for k in ("singles", "doubles", "mixed")}
    played = sum(split.values())
    if played >= 20:
        main = max(split, key=split.get)
        if split[main] / played >= 0.6:
            rec = records[main]["career"]
            background.append(f"Primarily a {main} player — {split[main]} of {played} career matches, "
                          f"winning {_pct(rec)}%.")

    # Current streak from the most recent results
    form = stats.get("form") or []
    if form:
        last = form[-1]
        streak = len(form) - len("".join(form).rstrip(last))
        if streak >= 3:
            points.append(f"{'Won' if last == 'W' else 'Lost'} their last {streak} matches on record.")

    titles = stats.get("titles") or []
    wins = [t for t in titles if t["result"] == "Winner"]
    finals = [t for t in titles if t["result"] == "Finalist"]
    this_season = [t for t in wins if t["year"] == season]
    if this_season:
        names = " and ".join(t["tournament"] for t in this_season[:2])
        lead = "including" if len(this_season) > 2 else "at"
        points.append(f"{len(this_season)} title{'s' if len(this_season) > 1 else ''} in {season}, {lead} {names}.")
    if wins or finals:
        since = min(t["year"] for t in titles if t["year"]) if any(t["year"] for t in titles) else ""
        parts = [f"{len(wins)} title{'s' if len(wins) != 1 else ''}"] if wins else []
        if finals:
            parts.append(f"{len(finals)} runner-up finish{'es' if len(finals) != 1 else ''}")
        points.append(f"{first}'s LTA record shows {' and '.join(parts)}" + (f" since {since}." if since else "."))
        repeat = {}
        for t in wins:
            repeat[t["tournament"]] = repeat.get(t["tournament"], 0) + 1
        best = max(repeat.items(), key=lambda kv: kv[1], default=None)
        if best and best[1] >= 2:
            times = {2: "twice", 3: "three times", 4: "four times", 5: "five times"}.get(best[1], f"{best[1]} times")
            points.append(f"Has won {best[0]} {times}.")
    elif career and sum(career) >= 20:
        points.append("Still chasing a first LTA title or final.")
    return points + background


def lta_candidate_keys(player):
    """Feed name formats an LTA entrant may appear under, as normalised keys."""
    surname, first = player["surname"], player["first"]
    keys = [f"{surname} {first[:1]}", f"{first} {surname}", f"{surname}, {first}", f"{surname} {first}"]
    return [normalize_player_key(k) for k in keys if first]


def match_lta_player_key(player, known_keys):
    """Existing feed/bio key for this entrant, else the feed's 'SURNAME I' style."""
    candidates = lta_candidate_keys(player)
    for key in candidates:
        if key in known_keys:
            return key
    # Compound surnames: the feed may use only the last part ("GIMENO P" for Patricia Gisbert Gimeno)
    last_part = player["surname"].split()[-1] if player["surname"].split() else ""
    if last_part and last_part != player["surname"] and player["first"]:
        short = normalize_player_key(f"{last_part} {player['first'][:1]}")
        if short in known_keys:
            return short
    # Truncated first names in the feed, e.g. "ELIZ MALONEY" for Elizabeth Maloney
    surname, first = player["surname"].upper(), player["first"].upper()
    for key in known_keys:
        rest = None
        if key.endswith(" " + surname):
            rest = key[:-len(surname) - 1]
        elif key.startswith(surname + ", "):
            rest = key[len(surname) + 2:]
        if rest and len(rest) > 1 and first.startswith(rest):
            return key
    return candidates[0] if candidates else ""


# Columns of a bios CSV (export, LTA scrape output, and upload). lta_first /
# lta_surname are optional: when present, rows are matched to the live feed's
# player names on import instead of trusting player_key.
BIO_CSV_COLUMNS = ("player_key", "lta_first", "lta_surname",
                   "display_name", "country", "born", "plays", "hometown", "career", "notes", "lta_url",
                   "lta_stats")


def scrape_lta_tournament(tournament_id, progress=None):
    """Fetch every entrant of an LTA tournament as bio CSV rows. Returns (rows, failed_names)."""
    session = lta_session()
    entrants = lta_tournament_players(session, tournament_id)
    if progress:
        progress(0, len(entrants))
    done = [0]

    def fetch(p):
        try:
            return p, lta_player_details(session, tournament_id, p["player_no"])
        except Exception as e:
            print(f"LTA scrape: failed for player {p['player_no']}: {e}")
            return p, None
        finally:
            done[0] += 1
            if progress:
                progress(done[0], len(entrants))

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(LTA_IMPORT_WORKERS) as pool:
        results = list(pool.map(fetch, entrants))

    rows, failed = [], []
    for player, details in results:
        if details is None:
            failed.append(f"{player['first']} {player['surname']}")
            continue
        rows.append({"player_key": (lta_candidate_keys(player) or [""])[0],
                     "lta_first": player["first"], "lta_surname": player["surname"],
                     **lta_bio_fields(player, details)})
    rows.sort(key=lambda r: (r["lta_surname"].upper(), r["lta_first"].upper()))
    return rows, failed


def merge_bio_rows(rows, overwrite=False):
    """
    Save bio rows into player_bios. Rows carrying LTA names are matched to the
    live feed's player keys; others use player_key as-is. Unless overwrite is
    set, only empty fields are filled so staff-entered text is never lost.
    Returns a summary string.
    """
    # Linked duplicate keys still match, and resolve to the player they were linked to
    known_keys = set(collect_known_players()) | set(manager.player_aliases)

    # A surname + initial shared by two rows can't be matched safely
    short_counts = {}
    for row in rows:
        if row.get("lta_surname"):
            short = lta_candidate_keys({"first": row.get("lta_first") or "", "surname": row["lta_surname"]})[:1]
            if short:
                short_counts[short[0]] = short_counts.get(short[0], 0) + 1

    created = updated = unchanged = 0
    skipped = []
    claimed = {}  # feed key -> row label, so two rows never merge into one bio
    for row in rows:
        surname = (row.get("lta_surname") or "").strip()
        if surname:
            label = f"{row.get('lta_first') or ''} {surname}".strip()
            key = match_lta_player_key({"first": (row.get("lta_first") or "").strip(), "surname": surname}, known_keys)
            if short_counts.get(key, 0) > 1:
                skipped.append(f"{label} (ambiguous name)")
                continue
        else:
            key = normalize_player_key(row.get("player_key") or row.get("display_name"))
            label = key
        if not key:
            skipped.append(f"{label or 'row'} (no name)")
            continue
        key = manager.resolve_player_key(key)
        if key in claimed:
            skipped.append(f"{label} (same feed name as {claimed[key]})")
            continue
        claimed[key] = label

        existing = manager.get_player_bio(key) or {}
        incoming = {c: str(row.get(c) or '').strip() for c in manager.PLAYER_BIO_FIELDS}
        if overwrite:
            fields = {c: incoming[c] or existing.get(c) or '' for c in manager.PLAYER_BIO_FIELDS}
        else:
            fields = {c: existing.get(c) or incoming[c] for c in manager.PLAYER_BIO_FIELDS}
        # Scraped stats are machine data: a newer scrape always refreshes them
        if incoming['lta_stats']:
            fields['lta_stats'] = incoming['lta_stats']
        if existing and all((existing.get(c) or '') == fields[c] for c in manager.PLAYER_BIO_FIELDS):
            unchanged += 1
            continue
        if manager.save_player_bio(key, fields):
            if existing:
                updated += 1
            else:
                created += 1
        else:
            skipped.append(f"{label} (save failed)")

    summary = (f"{created} new bio(s), {updated} updated, {unchanged} unchanged, {len(skipped)} skipped.")
    if skipped:
        summary += " Skipped: " + ", ".join(skipped)
    return summary


def bio_rows_to_csv(rows):
    import csv
    import io
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=BIO_CSV_COLUMNS, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def bio_rows_from_csv(text):
    import csv
    import io
    return list(csv.DictReader(io.StringIO(text.lstrip('﻿'))))


def run_lta_import(tournament_id):
    """Background job: scrape an LTA tournament straight into player_bios."""
    status = lta_import_status

    def progress(done, total):
        status.update(done=done, total=total, message=f"Fetched {done}/{total} LTA profiles…")

    try:
        rows, failed = scrape_lta_tournament(tournament_id, progress)
        status["message"] = "LTA import finished: " + merge_bio_rows(rows)
        if failed:
            status["message"] += " Fetch failed: " + ", ".join(failed)
    except Exception as e:
        status["error"] = f"LTA import failed: {e}"
    finally:
        status["running"] = False


@app.route('/api/v1/lta_import', methods=['GET', 'POST'])
def api_lta_import():
    """POST {url}: start importing an LTA tournament's players. GET: progress."""
    if request.method == 'POST':
        if manager is None:
            return jsonify({"error": "Cache manager not initialized."}), 503
        if lta_import_status["running"]:
            return jsonify({"error": "An LTA import is already running.", **lta_import_status}), 409
        source = (request.form.get('url') or (request.get_json(silent=True) or {}).get('url') or '').strip()
        found = LTA_TOURNAMENT_RE.search(source)
        if not found:
            return jsonify({"error": "Paste an LTA tournament link, e.g. "
                                     f"{LTA_BASE}/tournament/<id>/players"}), 400
        lta_import_status.update(running=True, done=0, total=0, error="",
                                 message="Opening LTA tournament…")
        Thread(target=run_lta_import, args=(found.group(1),), daemon=True).start()
    return jsonify(lta_import_status)


@app.route('/players/export.csv', methods=['GET'])
def players_export_csv():
    """Download every saved bio as CSV (backup, or to edit and re-upload)."""
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503
    rows = [dict(bio, player_key=key) for key, bio in sorted(manager.get_all_player_bios(include_aliases=False).items())]
    response = make_response(bio_rows_to_csv(rows))
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = 'attachment; filename=player_bios.csv'
    return response


# ====================================================================
# LTA order of play: pre-stage every court's matches from the LTA
# tournament "Matches" pages, linked to TennisTicker matches once live
# ====================================================================
LTA_SCHEDULE_REFRESH_MIN = 5
# {"tournament_id", "url", "fetched_at", "error", "matches": [...]} - persisted in app_settings
lta_schedule = {"tournament_id": "", "url": "", "fetched_at": 0, "error": "", "matches": []}
# LTA court name -> TennisTicker court name: "learned" from linked matches, "manual" from /config
lta_court_map = {"learned": {}, "manual": {}}
lta_schedule_lock = Lock()


def load_lta_schedule(mgr):
    """Restore the saved schedule and court map (called with the other persisted settings)."""
    for key, target in (("lta_schedule", lta_schedule), ("lta_court_map", lta_court_map), ("lta_staged", lta_staged),
                        ("lta_stage_prefs", lta_stage_prefs)):
        raw = mgr.get_setting(key)
        if raw:
            try:
                target.update(json.loads(raw))
            except ValueError:
                print(f"Ignoring unreadable setting {key}")
    if lta_schedule.get("tournament_name"):
        lta_schedule["tournament_name"] = _lta_tournament_name(lta_schedule["tournament_name"])
        if lta_schedule["tournament_name"].lower() == "matches":
            lta_schedule["tournament_name"] = ""


def save_lta_schedule():
    lta_version[0] += 1   # invalidates the cached staged rows
    if manager:
        manager.save_setting("lta_schedule", json.dumps(lta_schedule))
        manager.save_setting("lta_court_map", json.dumps(lta_court_map))
        manager.save_setting("lta_staged", json.dumps(lta_staged))
        manager.save_setting("lta_stage_prefs", json.dumps(lta_stage_prefs))


def _split_seed(text):
    m = re.match(r'^(.*?)\s*\[([^\]]+)\]$', text)
    return (m.group(1).strip(), m.group(2)) if m else (text.strip(), "")


def _parse_lta_match(block, date, time_label, index):
    titles = [_lta_text(x) for x in re.findall(r'match__header-title-item">(.*?)</li>', block, re.S)]
    draw = re.search(r'draw\.aspx\?id=[^&"]+&amp;draw=(\d+)', block)
    aside = re.search(r'match__header-aside-block[^"]*"\s+title="([^"]*)"', block)
    footer = re.search(r'match__footer-list-item">.*?nav-link__value">([^<]*)<', block, re.S)
    place = _lta_text(footer.group(1)) if footer else ""
    lta_court = place if " - " in place else ""
    duration = ""
    if aside:
        dm = re.search(r'Duration:\s*([^|]+)', aside.group(1))
        duration = dm.group(1).strip() if dm else ""

    body = block.split('match__row-wrapper', 1)[-1].split('match__result', 1)[0]
    sides, winner = [], 0
    # Row divs are exactly class="match__row " or "match__row has-won" (not match__row-title…)
    parts = re.split(r'<div class="match__row( has-won)?\s*">', body)
    for n, (won_flag, row) in enumerate(zip(parts[1::2], parts[2::2]), start=1):
        if n > 2:
            break
        if won_flag:
            winner = n
        players = []
        for pid, name in re.findall(r'player=(\d+)"[^>]*><span class="nav-link__value">([^<]+)</span>', row):
            name, seed = _split_seed(_lta_text(name))
            players.append({"id": pid, "name": name, "seed": seed})
        sides.append(players)
    while len(sides) < 2:
        sides.append([])

    result = block.split('match__result', 1)[-1].split('match__footer', 1)[0]
    sets = []
    for cells in re.findall(r'<ul class="points">(.*?)</ul>', result, re.S):
        nums = [int(x) for x in re.findall(r'points__cell[^"]*">\s*(\d+)', cells)]
        if len(nums) == 2:
            sets.append(nums)

    status = "COMPLETED" if winner else "LIVE" if sets else ("UPCOMING" if time_label else "UNSCHEDULED")
    ids = ["-".join(p["id"] for p in side) or "tbc" for side in sides]
    return {
        "key": f"{date}-{draw.group(1) if draw else 'x'}-{ids[0]}-{ids[1]}" + (f"-{index}" if "tbc" in ids else ""),
        "date": f"{date[:4]}-{date[4:6]}-{date[6:]}",
        "time": time_label,
        "event": titles[0] if titles else "",
        "round": " · ".join(titles[1:]),
        "lta_court": lta_court,
        "venue": place.split(" - ")[0] if place else "",
        "sides": sides,
        "winner": winner,
        "sets": sets,
        "duration": duration,
        "status": status,
        "tt_matchid": "",
    }


def lta_matches_in_day(session, tournament_id, date):
    """Every match on one day of the LTA order of play."""
    html = session.get(f"{LTA_BASE}/tournament/{tournament_id}/Matches/MatchesInDay", params={"date": date},
                       headers={"X-Requested-With": "XMLHttpRequest"}, timeout=30).text
    matches, time_label, index = [], "", 0
    for chunk in re.split(r'(<h5 class="sticky is-sticky match-group__header">.*?</h5>)', html, flags=re.S):
        if 'match-group__header' in chunk and chunk.startswith('<h5'):
            label = _lta_text(chunk)
            time_label = label if re.match(r'^\d{1,2}:\d{2}$', label) else ""
            continue
        for block in chunk.split('<div class="match match--list">')[1:]:
            index += 1
            matches.append(_parse_lta_match(block, date, time_label, index))
    return matches


def refresh_lta_schedule(source=None):
    """Re-pull the whole LTA order of play. Keeps TennisTicker links already made. Returns (ok, message)."""
    with lta_schedule_lock:
        if source:
            found = LTA_TOURNAMENT_RE.search(source)
            if not found:
                return False, f"Paste an LTA tournament link, e.g. {LTA_BASE}/tournament/<id>/Matches"
            if found.group(1).lower() != lta_schedule.get("tournament_id"):
                lta_schedule.update(matches=[], fetched_at=0)
                lta_court_map["learned"] = {}
            lta_schedule.update(tournament_id=found.group(1).lower(), url=source.strip())
        tid = lta_schedule.get("tournament_id")
        if not tid:
            return False, "No LTA tournament set."
        try:
            session = lta_session()
            page = session.get(f"{LTA_BASE}/tournament/{tid}/Matches", timeout=30).text
            days = sorted(set(re.findall(r'MatchesInDay\?date=(\d{8})', page)))
            title = re.search(r'<title>(.*?)</title>', page, re.S)
            tournament_name = _lta_tournament_name(_lta_text(title.group(1))) if title else ""
            matches = []
            for day in days:
                matches.extend(lta_matches_in_day(session, tid, day))
        except Exception as e:
            lta_schedule["error"] = f"LTA schedule refresh failed: {e}"
            save_lta_schedule()
            return False, lta_schedule["error"]

        links = {m["key"]: m.get("tt_matchid") for m in lta_schedule.get("matches", []) if m.get("tt_matchid")}
        for m in matches:
            m["tt_matchid"] = links.get(m["key"], "")
        lta_schedule.update(matches=matches, fetched_at=int(time.time()), error="", days=days,
                            tournament_name=tournament_name)
        save_lta_schedule()
    link_lta_schedule()
    notify_schedule_changed()
    return True, f"Loaded {len(matches)} LTA matches across {len(days)} day(s)."


def lta_schedule_tick():
    """Periodic background refresh (from the scraper loop's scheduler)."""
    if lta_schedule.get("tournament_id"):
        Thread(target=refresh_lta_schedule, daemon=True).start()


def _feed_name_key(name):
    """('SURNAME', 'I') from feed names: 'BYRNE J', 'Freya CHRISTIE', 'HARDIE, Ariana'."""
    name = re.sub(r'\s+', ' ', name).strip()
    if "," in name:
        surname, first = [x.strip() for x in name.split(",", 1)]
        return surname.upper(), first[:1].upper()
    parts = name.split(" ")
    if len(parts) > 1 and len(parts[-1]) == 1:
        return " ".join(parts[:-1]).upper(), parts[-1].upper()
    caps = [p for p in parts if p.isupper() and len(p) > 1]
    rest = [p for p in parts if p not in caps]
    if caps and rest:
        return " ".join(caps).upper(), rest[0][:1].upper()
    return " ".join(parts[1:]).upper(), parts[0][:1].upper()


def _lta_name_keys(name):
    """Every ('SURNAME', 'I') an LTA name could appear as: 'Patricia Gisbert Gimeno' -> GIMENO / GISBERT GIMENO."""
    parts = name.split()
    if len(parts) < 2:
        return {(name.upper(), "")}
    initial = parts[0][:1].upper()
    return {(" ".join(parts[i:]).upper(), initial) for i in range(1, len(parts))}


NAME_MATCH_THRESHOLD = 0.8   # surname similarity needed when the spellings differ


@functools.lru_cache(maxsize=50000)
def _player_similarity(feed_name, lta_name):
    """
    0..1: how well a feed name ("MCCARDLE P", "DELAWARE L") matches an LTA name
    ("Paul Mcardle", "Liam De La Mare"). First initials must agree; surnames are
    compared ignoring case, spaces and punctuation, allowing small spelling differences.
    """
    surname, initial = _feed_name_key(feed_name)
    flat = lambda s: re.sub(r"[^A-Z]", "", s.upper())
    fs = flat(surname)
    if not fs:
        return 0.0
    best = 0.0
    for lta_surname, lta_initial in _lta_name_keys(lta_name):
        if initial and lta_initial and initial != lta_initial:
            continue
        ls = flat(lta_surname)
        if not ls:
            continue
        if fs == ls:
            return 1.0
        best = max(best, difflib.SequenceMatcher(None, fs, ls).ratio())
    return best


def _side_score(feed_side, lta_side):
    """Lowest player similarity for the best pairing of the two sides (0 if they can't match)."""
    if not feed_side or len(feed_side) != len(lta_side):
        return 0.0
    names = [p["name"] for p in lta_side]
    best = 0.0
    for order in itertools.permutations(range(len(names))):
        score = min(_player_similarity(fp["name"], names[i]) for fp, i in zip(feed_side, order))
        best = max(best, score)
        if best == 1.0:
            break
    return best


def _sides_match(feed_side, lta_side):
    return _side_score(feed_side, lta_side) >= NAME_MATCH_THRESHOLD


def lta_match_score(fm, entry):
    """(orientation, score): orientation 1 = same side order, 2 = swapped, 0 = different players."""
    s1 = side_player_entries(fm.get("player1_full") or fm.get("player1"))
    s2 = side_player_entries(fm.get("player2_full") or fm.get("player2"))
    same = min(_side_score(s1, entry["sides"][0]), _side_score(s2, entry["sides"][1]))
    swapped = min(_side_score(s1, entry["sides"][1]), _side_score(s2, entry["sides"][0]))
    if max(same, swapped) < NAME_MATCH_THRESHOLD:
        return 0, 0.0
    return (1, same) if same >= swapped else (2, swapped)


def lta_orientation(fm, entry):
    """1 if the feed's side 1 is the LTA entry's side 1, 2 if the sides are swapped, 0 if the players differ."""
    return lta_match_score(fm, entry)[0]


def _pair_side(feed_side, lta_side):
    """Pair each feed player with the LTA player they are (best overall pairing)."""
    names = [p["name"] for p in lta_side]
    best, best_order = -1.0, None
    for order in itertools.permutations(range(len(names))):
        score = min((_player_similarity(fp["name"], names[i]) for fp, i in zip(feed_side, order)), default=0)
        if score > best:
            best, best_order = score, order
    return [(fp, lta_side[i]) for fp, i in zip(feed_side, best_order or [])]


def _lta_feed_style(feed_player, lta_player):
    """A player in the feed's "SURNAME I (CTRY)" form, spelt as LTA has it."""
    if _player_similarity(feed_player["name"], lta_player["name"]) == 1.0:
        name = feed_player["name"]   # same name: keep the feed's form so player keys don't change
    else:
        first, _, surname = lta_player["name"].partition(" ")
        name = f"{(surname or first).upper()} {first[:1].upper()}".strip()
    return name, (f"{name} ({feed_player['country']})" if feed_player.get("country") else name)


def apply_lta_preference(rows):
    """
    For TennisTicker matches linked to the LTA order of play, LTA's data wins:
    player names (LTA spelling, kept in the feed's format), event and round,
    tournament name, and - once LTA has recorded it - the final result. Live
    point-by-point scores still come from TennisTicker.
    """
    entries = {e["tt_matchid"]: e for e in lta_schedule.get("matches", []) if e.get("tt_matchid")}
    if not entries:
        return rows
    out = []
    for m in rows:
        e = entries.get(str(m.get("matchid") or ""))
        if not e or m.get("staged"):
            out.append(m)
            continue
        orientation, _score = lta_match_score(m, e)
        if not orientation:
            out.append(m)
            continue
        lta_sides = e["sides"] if orientation == 1 else [e["sides"][1], e["sides"][0]]
        n = dict(m)
        for idx in (1, 2):
            feed_side = side_player_entries(m.get(f"player{idx}_full") or m.get(f"player{idx}"))
            lta_side = lta_sides[idx - 1]
            if feed_side and len(feed_side) == len(lta_side):
                styled = [_lta_feed_style(fp, lp) for fp, lp in _pair_side(feed_side, lta_side)]
            else:
                styled = [_lta_feed_style({"name": "", "country": ""}, lp) for lp in lta_side]
            if not styled:
                continue
            raw = " / ".join(s[1] for s in styled)
            n[f"player{idx}"] = raw
            n[f"player{idx}_full"] = raw
            n[f"player{idx}_surname"] = " / ".join(_feed_name_key(s[0])[0] for s in styled)
        n["tt_matchname"] = m.get("matchname")
        n["matchname"] = " · ".join(x for x in (e["event"], e["round"]) if x) or m.get("matchname")
        if lta_schedule.get("tournament_name"):
            n["tname"] = lta_schedule["tournament_name"]
        n["lta_key"] = e["key"]
        n["lta_court"] = e["lta_court"]
        # LTA's recorded result is the official one
        if e["status"] == "COMPLETED" and e.get("winner") and e.get("sets"):
            for i in range(1, MAX_SETS + 1):
                for k in (f"set{i}_p1", f"set{i}_p2", f"set{i}_tb"):
                    n.pop(k, None)
            for i, (a, b) in enumerate(e["sets"][:MAX_SETS], start=1):
                n[f"set{i}_p1"], n[f"set{i}_p2"] = (a, b) if orientation == 1 else (b, a)
                n[f"set{i}_tb"] = ""
            n["sets_played_count"] = len(e["sets"])
            winner_side = e["winner"] if orientation == 1 else 3 - e["winner"]
            n["winner"] = str(winner_side)
            n["winner_name"] = n[f"player{winner_side}"]
            n["matchstatus"] = "(completed)"
            n["is_plan"] = 0
            n["game1"], n["game2"] = "", ""
            n["lta_result"] = True
        out.append(n)
    return out


def link_lta_schedule():
    """
    Attach TennisTicker match ids to LTA schedule entries: live matches (any day's
    unfinished entry, today first) and today's finished matches. Names may be spelt
    slightly differently between the two systems; the closest match is used.
    """
    if manager is None or not lta_schedule.get("matches"):
        return
    changed = False
    today = datetime.now().strftime("%Y-%m-%d")
    with lta_schedule_lock:
        linked = {m["tt_matchid"] for m in lta_schedule["matches"] if m.get("tt_matchid")}
        for fm in manager.get_latest_data():
            mid = str(fm.get("matchid") or "")
            tt_court = str(fm.get("court") or "").strip()
            status = classify_match_status(fm)
            if not mid or mid in linked or fm.get("staged") or status not in ("LIVE", "COMPLETED"):
                continue
            candidates = []
            for e in lta_schedule["matches"]:
                if e.get("tt_matchid"):
                    continue
                # Live: an unfinished entry (any day, today preferred). Finished: today's entry.
                if status == "LIVE" and e["status"] == "COMPLETED" and e["date"] != today:
                    continue
                if status == "COMPLETED" and e["date"] != today:
                    continue
                orientation, score = lta_match_score(fm, e)
                if orientation:
                    candidates.append((score, e))
            if not candidates:
                continue
            # Closest names first, then today's entry, then the earliest scheduled
            score, entry = sorted(candidates, key=lambda c: (-c[0], c[1]["date"] != today, c[1]["date"],
                                                             c[1]["time"] or "99:99"))[0]
            entry["tt_matchid"] = mid
            linked.add(mid)
            changed = True
            # A scorer working on the staged match carries on with the TennisTicker match
            staged_id = STAGED_PREFIX + entry["key"]
            state = manager.manual_scores.get(staged_id)
            if state is not None and mid not in manager.manual_scores:
                manager.save_manual_score(mid, state, active=True)
                manager.save_manual_score(staged_id, state, active=False)
                print(f"Manual scoring moved from {staged_id} to TennisTicker match {mid}")
            if entry["lta_court"] and tt_court:
                lta_court_map["learned"][entry["lta_court"]] = tt_court
            note = "" if score == 1.0 else f" (names matched {round(score * 100)}%)"
            print(f"LTA schedule: linked TennisTicker match {mid} ({status.lower()}) to {entry['event']} "
                  f"on {entry['lta_court'] or 'unknown court'}{note}")
        if changed:
            save_lta_schedule()
    if changed:
        notify_schedule_changed()


def lta_display_court(lta_court):
    return (lta_court_map["manual"].get(lta_court) or lta_court_map["learned"].get(lta_court) or lta_court)


def _lta_side_name(side):
    return " / ".join(p["name"] for p in side) or "TBC"


# --------------------------------------------------------------------
# Staging: an LTA match pushed onto a court as an upcoming match row
# --------------------------------------------------------------------
STAGED_PREFIX = "lta-"
# {lta_key: {"court": tt_court, "time": "HH:MM"}} - persisted with the schedule
lta_staged = {}
# Auto-staging: every match on today's order of play with a court is staged unless the
# operator unstaged it ("skipped"), so scorers can pick any of them on /score.
lta_stage_prefs = {"auto": True, "skipped": []}
lta_version = [0]          # bumped whenever schedule / staging data changes
_staged_cache = {"key": None, "value": ([], set())}


def effective_staged():
    """{lta_key: {"court", "time", "auto"}}: explicitly staged matches plus today's auto-staged ones."""
    out = {}
    if lta_stage_prefs.get("auto", True):
        today = datetime.now().strftime("%Y-%m-%d")
        skipped = set(lta_stage_prefs.get("skipped") or [])
        for e in lta_schedule.get("matches", []):
            if (e["date"] == today and e["lta_court"] and e["status"] != "COMPLETED" and e["key"] not in skipped
                    and all(e["sides"][i] for i in (0, 1))):
                out[e["key"]] = {"court": "", "time": "", "auto": True}
    for key, stage in lta_staged.items():
        out[key] = dict(stage, auto=False)
    # A staged match a scorer has worked on stays in the data until they hand it back
    if manager:
        for mid in manager.manual_scores:
            if mid.startswith(STAGED_PREFIX) and mid[len(STAGED_PREFIX):] not in out:
                out[mid[len(STAGED_PREFIX):]] = {"court": "", "time": "", "auto": True, "scored": True}
    return out


def _lta_feed_side(side):
    """LTA players as a feed-style side string the rest of the app already parses."""
    return " / ".join(p["name"] for p in side) or "TBC"


COUNTRY_CODES = {
    "great britain": "GBR", "united kingdom": "GBR", "england": "GBR", "scotland": "GBR", "wales": "GBR",
    "ireland": "IRL", "spain": "ESP", "france": "FRA", "germany": "GER", "italy": "ITA", "netherlands": "NED",
    "portugal": "POR", "sweden": "SWE", "belgium": "BEL", "switzerland": "SUI", "united states": "USA",
    "australia": "AUS", "argentina": "ARG", "brazil": "BRA", "denmark": "DEN", "norway": "NOR", "finland": "FIN",
}


def _staged_side(side, bios):
    """(full names, feed-style country code) for an LTA side, using saved bios where we have them."""
    names, countries = [], set()
    for p in side:
        first, _, surname = p["name"].partition(" ")
        bio = {}
        for key in lta_candidate_keys({"first": first, "surname": surname or first}):
            if key in bios:
                bio = bios[key]
                break
        names.append(bio.get("display_name") or p["name"])
        country = str(bio.get("country") or "").strip()
        countries.add(COUNTRY_CODES.get(country.lower(), country.upper() if len(country) == 3 else ""))
    code = countries.pop() if len(countries) == 1 else ""
    return " / ".join(names) or "TBC", code


def lta_sport():
    """'padel' or 'tennis' for the loaded LTA tournament (from its name)."""
    return "padel" if "padel" in (lta_schedule.get("tournament_name") or "").lower() else "tennis"


def _lta_tournament_name(title_text):
    """'Matches - LTA Padel National Championships 2026 | LTA - Tennis for Britain' -> the tournament name."""
    name = title_text.split(" | ")[0].strip()
    if " - " in name and name.split(" - ", 1)[0].strip().lower() in ("matches", "players", "draws", "events", "overview"):
        name = name.split(" - ", 1)[1].strip()
    return name


def staged_match_rows(feed_rows):
    """
    Synthetic upcoming match rows for staged LTA matches, built from our own data
    (LTA order of play + player bios). A staged match stays on air - even if
    TennisTicker also lists it as planned - until TennisTicker marks it live
    (or finishes it), or LTA records a result. Returns (rows, covered_feed_ids):
    covered ids are TennisTicker's planned duplicates of staged matches.
    """
    staged_set = effective_staged()
    if not staged_set:
        return [], set()
    # Recomputed only when the feed cache, schedule/staging, bios or the date change
    cache_key = (id(feed_rows), len(feed_rows), lta_version[0], datetime.now().strftime("%Y-%m-%d"),
                 id(manager._load_player_bios()) if manager else 0, len(manager.player_aliases) if manager else 0,
                 len(staged_set), len(manager.manual_scores) if manager else 0)
    if _staged_cache["key"] == cache_key:
        return _staged_cache["value"]
    entries = {e["key"]: e for e in lta_schedule.get("matches", [])}
    feed_by_id = {str(m.get("matchid")): m for m in feed_rows}
    today = datetime.now().strftime("%Y-%m-%d")
    bios = manager.get_all_player_bios() if manager else {}
    tname_default = lta_schedule.get("tournament_name") or next(
        (str(m.get("tname")).strip() for m in feed_rows if str(m.get("tname") or "").strip()), "")
    rows, covered = [], set()
    for key, stage in list(staged_set.items()):
        e = entries.get(key)
        scored = manager is not None and (STAGED_PREFIX + key) in manager.manual_scores
        if not e or (e["status"] == "COMPLETED" and not scored):
            continue
        # The feed's copy of this match: linked by id, or the same players today
        twins = [feed_by_id[e["tt_matchid"]]] if e.get("tt_matchid") in feed_by_id else []
        if e["date"] == today:
            twins += [fm for fm in feed_rows if not fm.get("staged") and lta_orientation(fm, e)]
        if any(classify_match_status(fm) != "UPCOMING" for fm in twins) and not scored:
            continue   # TennisTicker has it live (or finished): its live data takes over
        covered.update(str(fm.get("matchid")) for fm in twins)

        (p1, c1), (p2, c2) = _staged_side(e["sides"][0], bios), _staged_side(e["sides"][1], bios)
        court = stage.get("court") or lta_display_court(e["lta_court"]) or e["venue"]
        same_court_tname = next((str(fm.get("tname")).strip() for fm in feed_rows
                                 if str(fm.get("court") or "") == court and str(fm.get("tname") or "").strip()), "")
        rows.append({
            "matchid": STAGED_PREFIX + key,
            "staged": True,
            "auto_staged": bool(stage.get("auto")),
            "lta_key": key,
            "court": court,
            "schedtime": stage.get("time") or e["time"],
            "schedule_date": e["date"],
            "matchname": " ".join(x for x in (e["event"], e["round"]) if x),
            "tname": lta_schedule.get("tournament_name") or same_court_tname or tname_default,
            "sport": lta_sport(),
            "tournid": "",
            "player1": p1, "player2": p2, "player1_full": p1, "player2_full": p2,
            "player1_surname": side_surnames(p1, bios).upper(), "player2_surname": side_surnames(p2, bios).upper(),
            "player1_country": c1, "player2_country": c2,
            "matchstatus": "UPCOMING", "is_plan": 1, "winner": "", "winner_name": "",
            "sets_played_count": 0, "game1": "", "game2": "", "player2serve": 0,
            "timestamp": int(time.time()),
        })
    _staged_cache.update(key=cache_key, value=(rows, covered))
    return rows, covered


def staged_match_alias(matchid):
    """
    The TennisTicker match id a staged match ("lta-…") became: its linked id, or
    the feed match with the same players today. None if it's still only staged.
    """
    matchid = str(matchid or "")
    if not matchid.startswith(STAGED_PREFIX) or manager is None:
        return None
    entry = next((e for e in lta_schedule.get("matches", []) if e["key"] == matchid[len(STAGED_PREFIX):]), None)
    if not entry:
        return None
    feed = manager.get_latest_data(all_tournaments=True)
    ids = {str(m.get("matchid")) for m in feed}
    if entry.get("tt_matchid") in ids:
        return entry["tt_matchid"]
    today = datetime.now().strftime("%Y-%m-%d")
    if entry["date"] == today:
        twin = next((m for m in feed if not m.get("staged") and lta_orientation(m, entry)), None)
        if twin:
            return str(twin.get("matchid"))
    return None


def staged_ids_for(tt_matchid):
    """Staged ids ("lta-…") whose match is now this TennisTicker match."""
    return [STAGED_PREFIX + e["key"] for e in lta_schedule.get("matches", [])
            if e.get("tt_matchid") == str(tt_matchid)]


def set_lta_staged(keys, staged=True, court=None, time_label=None):
    """Stage or unstage LTA matches, then push the change to every screen."""
    entries = {e["key"] for e in lta_schedule.get("matches", [])}
    changed = 0
    for key in keys:
        if key not in entries:
            continue
        skipped = lta_stage_prefs.setdefault("skipped", [])
        if staged:
            if court is not None and court.strip().lower() in ("", "unassigned"):
                continue
            if key in skipped:
                skipped.remove(key)
            current = lta_staged.get(key, {})
            lta_staged[key] = {"court": (court if court is not None else current.get("court", "")).strip(),
                               "time": (time_label if time_label is not None else current.get("time", "")).strip()}
        else:
            lta_staged.pop(key, None)
            if key not in skipped:
                skipped.append(key)   # keeps auto-staging from putting it straight back
        changed += 1
    if changed:
        save_lta_schedule()
        notify_schedule_changed()
    return changed


def notify_schedule_changed():
    """Tell open dashboards / schedule pages to re-read the schedule, and graphics the new match list."""
    socketio.emit('schedule_updated', {"timestamp": datetime.now().strftime('%H:%M:%S')}, to='dashboard')
    if manager:
        manager.broadcast_matches([])


def schedule_rows():
    """
    Merged per-match rows: LTA order of play (linked TennisTicker matches take
    their live status and score from the feed) plus TennisTicker matches that
    aren't in the LTA data, including the feed's own upcoming (plan) matches.
    """
    feed = {str(m.get("matchid")): m for m in (manager.get_latest_data() if manager else [])}
    bios = manager.get_all_player_bios() if manager else {}
    today = datetime.now().strftime("%Y-%m-%d")
    staged_set = effective_staged()
    rows, used = [], set()

    def feed_fields(fm):
        status = classify_match_status(fm)
        winner_side = 1 if fm.get("winner") == "1" else 2 if fm.get("winner") == "2" else 0
        return {
            "status": status,
            "score": match_score_line(fm),
            "p1_points": str(fm.get("game1") or "") if status == "LIVE" else "",
            "p2_points": str(fm.get("game2") or "") if status == "LIVE" else "",
            "winner": str(winner_side or ""),
            "manual": bool(fm.get("manual")),
        }

    # Feed matches not linked while live (e.g. finished before the schedule was loaded)
    # are still paired with their LTA entry for display, by players on the same day
    entries = lta_schedule.get("matches", [])
    linked_ids = {e.get("tt_matchid") for e in entries if e.get("tt_matchid")}
    display_link = {}

    def same_score(fm, e, orientation):
        """A finished feed match and an LTA entry with the same set scores (any day)."""
        feed_sets = [s.split("(")[0] for s in match_score_line(fm).split()]
        lta_sets = [f"{a}-{b}" if orientation == 1 else f"{b}-{a}" for a, b in e["sets"]]
        return bool(feed_sets) and feed_sets == lta_sets

    for mid, fm in feed.items():
        if mid in linked_ids or fm.get("staged") or classify_match_status(fm) == "UPCOMING":
            continue
        for e in entries:
            if e.get("tt_matchid") or e["key"] in display_link:
                continue
            orientation = lta_orientation(fm, e)
            # Same players today, or (for a finished match on another day) the same players and score
            if orientation and (e["date"] == today or
                                (classify_match_status(fm) == "COMPLETED" and same_score(fm, e, orientation))):
                display_link[e["key"]] = mid
                break

    for e in entries:
        fm = feed.get(e.get("tt_matchid") or display_link.get(e["key"]) or "")
        staged_fm = feed.get(STAGED_PREFIX + e["key"])   # staged and not yet carried by TennisTicker
        sides = e["sides"]
        if fm and lta_orientation(fm, e) == 2:
            sides = [sides[1], sides[0]]   # follow the feed's side order so names line up with its score
        p1, p2 = _lta_side_name(sides[0]), _lta_side_name(sides[1])
        row = {
            "court": str(fm.get("court")) if fm else lta_display_court(e["lta_court"]),
            "lta_court": e["lta_court"],
            "date": e["date"],
            "time": e["time"],
            "event": e["event"],
            "round": e["round"],
            "p1_full_name": p1,
            "p2_full_name": p2,
            "p1_seed": "/".join(p["seed"] for p in sides[0] if p["seed"]),
            "p2_seed": "/".join(p["seed"] for p in sides[1] if p["seed"]),
            "status": e["status"],
            "score": " ".join(f"{a}-{b}" for a, b in e["sets"]),
            "p1_points": "", "p2_points": "",
            "winner": str(e["winner"] or ""),
            "manual": False,
            "matchid": str(fm.get("matchid")) if fm else "",
            "lta_key": e["key"],
            "source": "lta",
            "staged": e["key"] in staged_set,
            "auto_staged": bool(staged_set.get(e["key"], {}).get("auto")),
            "stage_court": staged_set.get(e["key"], {}).get("court", ""),
            "stage_time": staged_set.get(e["key"], {}).get("time", ""),
        }
        if fm:
            used.add(str(fm.get("matchid")))
            row.update(feed_fields(fm), source="lta+tennisticker")
        elif staged_fm:
            used.add(staged_fm["matchid"])
            row.update(feed_fields(staged_fm), source="lta (staged)", matchid=staged_fm["matchid"],
                       court=staged_fm["court"], time=staged_fm["schedtime"])
            if row["status"] == "UPCOMING" and e["status"] == "UNSCHEDULED" and not row["time"]:
                row["status"] = "UNSCHEDULED"
        row["winner_full_name"] = p1 if row["winner"] == "1" else p2 if row["winner"] == "2" else ""
        rows.append(row)

    for mid, fm in feed.items():
        if mid in used or fm.get("staged"):
            continue
        p1 = full_side_name(fm.get("player1_full") or fm.get("player1"), bios)
        p2 = full_side_name(fm.get("player2_full") or fm.get("player2"), bios)
        row = {
            "court": str(fm.get("court") or ""), "lta_court": "", "date": today,
            "time": str(fm.get("schedtime") or ""), "event": str(fm.get("matchname") or ""), "round": "",
            "p1_full_name": p1, "p2_full_name": p2, "p1_seed": "", "p2_seed": "",
            "matchid": mid, "lta_key": "", "source": "tennisticker",
            "staged": False, "stage_court": "", "stage_time": "",
        }
        row.update(feed_fields(fm))
        row["winner_full_name"] = p1 if row["winner"] == "1" else p2 if row["winner"] == "2" else ""
        rows.append(row)

    order = {"LIVE": 0, "UPCOMING": 1, "UNSCHEDULED": 2, "COMPLETED": 3}
    rows.sort(key=lambda r: (r["date"], r["time"] or "99:99", order.get(r["status"], 1), r["court"]))
    return rows


def _filter_schedule_date(rows, when):
    """?date=today (default) | upcoming | all | YYYY-MM-DD."""
    today = datetime.now().strftime("%Y-%m-%d")
    when = (when or "today").lower()
    if when == "all":
        return rows
    if when == "upcoming":
        return [r for r in rows if r["date"] >= today and r["status"] != "COMPLETED"]
    if when == "today":
        when = today
    return [r for r in rows if r["date"] == when]


def _court_tokens(name):
    return {str(int(t)) if t.isdigit() else t for t in re.split(r'\W+', str(name).lower()) if t}


@app.route('/api/v1/schedule', methods=['GET'])
def api_schedule():
    """Order of play for every court: {"courts": {court: [rows]}}. ?date=today|upcoming|all|YYYY-MM-DD."""
    rows = _filter_schedule_date(schedule_rows(), request.args.get("date"))
    courts = {}
    for r in rows:
        courts.setdefault(r["court"] or "Unassigned", []).append(r)
    return jsonify({
        "status": "success",
        "lta_tournament": lta_schedule.get("url") or "",
        "lta_fetched_at": datetime.fromtimestamp(lta_schedule["fetched_at"]).strftime('%Y-%m-%d %H:%M:%S')
        if lta_schedule.get("fetched_at") else "",
        "court_count": len(courts),
        "courts": {c: courts[c] for c in sorted(courts, key=court_sort_key)},
    })


def resolve_name_to_player_key(name, known):
    """Our player key for a display name ("John Byrne" -> "BYRNE J"), or ''."""
    key = normalize_player_key(name)
    if manager:
        key = manager.resolve_player_key(key)
    if key in known:
        return key
    first, _, surname = name.partition(" ")
    candidates = lta_candidate_keys({"first": first, "surname": surname or first})
    if len(surname.split()) > 1:   # compound surname: the feed may use the last part ("GIMENO P")
        candidates.append(normalize_player_key(f"{surname.split()[-1]} {first[:1]}"))
    for candidate in candidates:
        candidate = manager.resolve_player_key(candidate) if manager else candidate
        if candidate in known:
            return candidate
    return ""


@app.route('/api/v1/search', methods=['GET'])
def api_search():
    """
    Player / match search for the commentary screen: every match (LTA order of play
    merged with TennisTicker, all days) involving a player whose name matches ?q=.
    """
    q = (request.args.get("q") or "").strip().lower()
    tokens = [x for x in re.split(r"\s+", q) if x]
    if len(q) < 2:
        return jsonify({"query": q, "players": [], "matches": []})

    def matches_name(name):
        n = name.lower()
        return all(tok in n for tok in tokens)

    known = set(collect_known_players()) | set(manager.get_all_player_bios() if manager else {})
    players, rows = {}, []
    for r in schedule_rows():
        hit_side, hit_names = 0, []
        for side, side_name in ((1, r["p1_full_name"]), (2, r["p2_full_name"])):
            names = [x.strip() for x in side_name.split(" / ") if x.strip()]
            found = [x for x in names if matches_name(x)]
            if found:
                hit_side = hit_side or side
                hit_names += found
        if not hit_side:
            continue
        result = ""
        if r["status"] == "COMPLETED" and r["winner"] in ("1", "2"):
            result = "W" if r["winner"] == str(hit_side) else "L"
        rows.append(dict(r, match_side=hit_side, players=hit_names, result=result))
        for name in hit_names:
            entry = players.setdefault(name, {"name": name, "key": resolve_name_to_player_key(name, known), "matches": 0})
            entry["matches"] += 1

    order = {"LIVE": 0, "UPCOMING": 1, "UNSCHEDULED": 2, "COMPLETED": 3}
    upcoming = sorted([r for r in rows if r["status"] != "COMPLETED"],
                      key=lambda r: (order.get(r["status"], 1), r["date"], r["time"] or "99:99"))
    done = sorted([r for r in rows if r["status"] == "COMPLETED"], key=lambda r: (r["date"], r["time"] or ""), reverse=True)
    return jsonify({
        "query": q,
        "players": sorted(players.values(), key=lambda p: (-p["matches"], p["name"])),
        "matches": (upcoming + done)[:300],
    })


@app.route('/api/v1/schedule/stage', methods=['POST'])
def api_schedule_stage():
    """
    Stage / unstage LTA matches as upcoming matches on a court.
    JSON {"keys": [...] or "key": "...", "staged": true, "court": "LTA-OC-1", "time": "16:30"}
    """
    body = request.get_json(silent=True) or {}
    keys = body.get("keys") or ([body["key"]] if body.get("key") else [])
    court = body.get("court")
    time_label = body.get("time")
    if time_label and not re.match(r'^\d{1,2}:\d{2}$', str(time_label).strip()):
        return jsonify({"error": "Time must look like 16:30."}), 400
    with lta_schedule_lock:
        changed = set_lta_staged([str(k) for k in keys], staged=bool(body.get("staged", True)),
                                 court=None if court is None else str(court),
                                 time_label=None if time_label is None else str(time_label))
    return jsonify({"status": "success", "changed": changed, "staged_count": len(lta_staged)})


@app.route('/api/v1/schedule/refresh', methods=['POST'])
def api_schedule_refresh():
    """Re-pull the LTA order of play now."""
    ok, msg = refresh_lta_schedule()
    return jsonify({"status": "success" if ok else "error", "message": msg}), (200 if ok else 400)


@app.route('/schedule')
def schedule_page():
    """Order of play for every court, with staging of upcoming matches onto courts."""
    tt_courts = sorted({str(m.get("court")) for m in (manager.get_latest_data(all_tournaments=True) if manager else [])
                        if m.get("court") and not m.get("staged")}
                       | set(lta_court_map["learned"].values()) | set(lta_court_map["manual"].values()),
                       key=court_sort_key)
    response = make_response(render_template(
        'schedule.html',
        lta_url=lta_schedule.get("url") or "",
        tournament_name=lta_schedule.get("tournament_name") or "",
        days=[f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in lta_schedule.get("days") or []]
        or sorted({m["date"] for m in lta_schedule.get("matches") or []}),
        tt_courts=tt_courts,
        pinned_courts=pinned_courts_for_display(),
    ))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


@app.route('/api/v1/schedule/court/<path:court>', methods=['GET'])
def api_schedule_court(court):
    """
    One court's event list as a flat JSON array (vMix Data Source friendly), in
    order of play, numbered by "slot". Matches the TennisTicker court name or the
    LTA court name; "1" matches "LTA-OC-1" and "Rocket Padel Bristol - 01".
    """
    wanted = court.strip().lower()
    tokens = _court_tokens(court)
    rows = [
        r for r in _filter_schedule_date(schedule_rows(), request.args.get("date"))
        if wanted in (r["court"].lower(), r["lta_court"].lower())
        or (len(tokens) == 1 and (tokens <= _court_tokens(r["court"]) or tokens <= _court_tokens(r["lta_court"])))
    ]
    return jsonify([{"slot": str(i), **{k: (v if isinstance(v, str) else str(v).lower() if isinstance(v, bool) else str(v))
                                        for k, v in r.items()}}
                    for i, r in enumerate(rows, start=1)])


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

    player_key = manager.resolve_player_key(player_key)
    bio = manager.get_player_bio(player_key) or {}
    wins, losses, results = compute_player_record(player_key)
    event_stats = player_event_stats(player_key)

    if not bio and not results:
        return jsonify({"error": f"No data for player '{player_key}'."}), 404

    display_name = bio.get('display_name') or player_key.title()
    # Prefer the exact feed casing when we have seen the player in a match
    known = collect_known_players().get(player_key)
    if known and not bio.get('display_name'):
        display_name = _feed_full_name(known['name'])   # "BUSH, Tegan" -> "Tegan Bush"

    last_completed = next((r for r in results if r['result']), None)
    lta = parse_lta_stats(bio)

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
        "lta": lta,
        "event_stats": event_stats,
        # This-event points first (most relevant live), then the LTA career points
        "talking_points": event_talking_points(event_stats, display_name) + lta_talking_points(lta, display_name),
        "tournament_wins": wins,
        "tournament_losses": losses,
        "tournament_record": f"{wins}-{losses}",
        "last_result": (
            f"{last_completed['result']} vs {last_completed['opponent']} {last_completed['score']}".strip()
            if last_completed else ''
        ),
        "matches": results
    })


def possible_duplicate_players(player_rows):
    """
    Entries that look like the same person: same surname + first initial
    ("BYRNE J" / "John Byrne") or the same saved full name. Groups marked as
    different people on /players are left out.
    """
    dismissed = set(json.loads(manager.get_setting('player_duplicates_dismissed') or '[]')) if manager else set()
    groups = {}
    for row in player_rows:
        names = {row["key"], row["name"], (row["bio"] or {}).get("display_name") or ""}
        sigs = set()
        for n in names:
            if not n or n.upper() in ("TBC", "BYE"):
                continue
            surname, initial = _feed_name_key(n)
            if surname and initial:
                sigs.add(f"{surname}|{initial}")
        for sig in sigs:
            groups.setdefault(sig, {})[row["key"]] = row
    seen, out = set(), []
    for sig, members in groups.items():
        if len(members) < 2:
            continue
        keys = tuple(sorted(members))
        group_id = "+".join(keys)
        if keys in seen or group_id in dismissed:
            continue
        seen.add(keys)
        rows = sorted(members.values(), key=lambda r: (not r["has_bio"], -len([v for v in (r["bio"] or {}).values() if v])))
        out.append({"id": group_id, "players": rows})
    return out


@app.route('/players', methods=['GET', 'POST'])
def players_page():
    """Player bio editor: production staff maintain commentator spotter data."""
    message = None
    error = None

    if request.method == 'POST':
        form_name = (request.form.get('form_name') or '').strip().lower()

        if form_name in ('link_players', 'unlink_player', 'dismiss_duplicate') and manager is None:
            error = "Database not ready yet - try again shortly."
        elif form_name == 'link_players':
            keep = normalize_player_key(request.form.get('keep_key'))
            results = [manager.link_players(normalize_player_key(dup), keep)
                       for dup in request.form.getlist('duplicate_key') if dup.strip()]
            failed = [msg for ok, msg in results if not ok]
            if failed or not results:
                error = " ".join(failed) or "Choose a player to link."
            else:
                message = " ".join(msg for _ok, msg in results)
        elif form_name == 'unlink_player':
            ok, msg = manager.unlink_player(normalize_player_key(request.form.get('alias_key')))
            message, error = (msg, None) if ok else (None, msg)
        elif form_name == 'dismiss_duplicate':
            dismissed = set(json.loads(manager.get_setting('player_duplicates_dismissed') or '[]'))
            dismissed.add(request.form.get('group', ''))
            manager.save_setting('player_duplicates_dismissed', json.dumps(sorted(dismissed)))
            message = "Marked as different players."
        elif form_name == 'bulk_names':
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
        elif form_name == 'import_csv':
            upload = request.files.get('bios_file')
            if manager is None:
                error = "Database not ready yet - try again shortly."
            elif not upload or not upload.filename:
                error = "Choose a bios CSV file to import."
            else:
                try:
                    rows = bio_rows_from_csv(upload.read().decode('utf-8-sig'))
                    message = f"Imported {upload.filename}: " + merge_bio_rows(
                        rows, overwrite=request.form.get('overwrite') == '1')
                except Exception as e:
                    error = f"Could not import {upload.filename}: {e}"
        else:
            player_key = normalize_player_key(request.form.get('player_key') or request.form.get('display_name'))
            if not player_key:
                error = "Player name is required."
            elif manager is None:
                error = "Database not ready yet - try again shortly."
            else:
                fields = {col: request.form.get(col, '') for col in manager.PLAYER_BIO_FIELDS}
                # Scraped LTA stats aren't editable in the form; keep them
                fields['lta_stats'] = (manager.get_player_bio(player_key) or {}).get('lta_stats') or ''
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
            "bio": bios.get(key) or {},
            "aliases": manager.aliases_of(key) if manager else [],
        }
        for key, info in sorted(players.items(), key=lambda kv: kv[1]["name"].upper())
    ]

    response = make_response(render_template(
        'players.html',
        players=player_rows,
        duplicates=possible_duplicate_players(player_rows),
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


def _feed_full_name(name):
    """Feed's "LESA, Giulia" / "KORPANEC DAVIES, Hermione" -> "Giulia Lesa" / "Hermione Korpanec Davies"."""
    surname, sep, first = name.partition(",")
    # Only flip real "Surname, First" names, not team entries like "Bath Doubles, 1 (W)"
    if not sep or not re.fullmatch(r"[^\W\d_][^\d(),]*", first.strip()):
        return name
    if surname.isupper():
        surname = re.sub(r"[A-Za-z]+", lambda w: w.group(0).capitalize(), surname)
    return f"{first.strip()} {surname.strip()}"


def _nice_case(word):
    """'MCGILL' / 'Mcgill' -> 'McGill', "O'NEIL" -> "O'Neil", 'JOHNSON-HAULDREN' -> 'Johnson-Hauldren'."""
    word = re.sub(r"[A-Za-z]+", lambda m: m.group(0).capitalize(), word.lower())
    # "Mc" is reliably followed by a capital; "Mac" isn't (Macey, Machin), so it's left alone
    return re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), word)


def side_surnames(raw, bios):
    """Surnames only for a side ("Byrne / McGill"), cased from the saved full name where possible."""
    out = []
    for p in side_player_entries(raw):
        name = p["name"]
        if name.upper() == "TBC":
            out.append("TBC")
            continue
        surname, sep, first = name.partition(",")
        # Team entries like "Bath Doubles, 1 (W)" aren't "Surname, First" - keep them whole
        if sep and not re.fullmatch(r"[^\W\d_][^\d(),]*", first.strip()):
            out.append(name)
            continue
        upper, _initial = _feed_name_key(name)
        cased = ""
        for source in ((bios.get(p["key"]) or {}).get("display_name") or "", name):
            i = source.upper().find(upper)
            if upper and i >= 0 and not source[i:i + len(upper)].isupper():
                cased = source[i:i + len(upper)]
                break
        if cased and cased == cased.capitalize():
            cased = _nice_case(cased)   # plain "Mcgill" from LTA data -> "McGill"
        out.append(cased or _nice_case(upper or name))
    return " / ".join(out)


def _fix_mc(name):
    """'Paul Mcardle' -> 'Paul McArdle' (LTA lists some names with plain capitalisation)."""
    return re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), name)


def full_side_name(raw, bios):
    """Side name using each player's saved full name ("William Skidelsky / Aled Smith"), feed name as fallback."""
    return " / ".join(
        _fix_mc((bios.get(p["key"]) or {}).get("display_name") or _feed_full_name(p["name"]))
        for p in side_player_entries(raw)
    )


def vmix_flat_row(match, sets_to_include=VMIX_SETS_PER_ROW, bios=None):
    """
    Flatten one cached match into a single-level dict of strings,
    ready for direct field mapping in vMix Data Sources.
    bios ({player_key: bio}) supplies full names; pass it once per request.
    """
    bios = bios or {}
    status = classify_match_status(match)
    is_live = (status == "LIVE")

    # player2serve: 1 = player 1 serving, 2 = player 2 serving (TennisTicker convention)
    serve = str(match.get("player2serve") or "")
    p1_serve = "1" if (is_live and serve == "1") else ""
    p2_serve = "1" if (is_live and serve == "2") else ""

    p1_raw = match.get("player1_full") or match.get("player1")
    p2_raw = match.get("player2_full") or match.get("player2")
    winner = str(match.get("winner_name") or "")
    winner_side = (1 if winner and winner in (str(match.get("player1_full") or ""), str(match.get("player1") or ""))
                   else 2 if winner and winner in (str(match.get("player2_full") or ""), str(match.get("player2") or ""))
                   else 0)

    row = {
        "matchid": str(match.get("matchid") or ""),
        "court": str(match.get("court") or ""),
        "status": status,
        "matchname": str(match.get("matchname") or ""),
        "tournament": str(match.get("tname") or ""),
        "schedtime": str(match.get("schedtime") or ""),
        "winner_name": str(match.get("winner_name") or ""),

        # Surnames only for graphics ("Byrne / McGill"); full names are in p1_full_name / p2_full_name
        "p1_name": side_surnames(p1_raw, bios),
        "p2_name": side_surnames(p2_raw, bios),
        "p1_surname": str(match.get("player1_surname") or ""),
        "p2_surname": str(match.get("player2_surname") or ""),
        "p1_country": str(match.get("player1_country") or ""),
        "p2_country": str(match.get("player2_country") or ""),
        # Full names from player bios (/players), feed name where none is saved
        "p1_full_name": full_side_name(p1_raw, bios),
        "p2_full_name": full_side_name(p2_raw, bios),
        "winner_full_name": (full_side_name(p1_raw, bios) if winner_side == 1
                             else full_side_name(p2_raw, bios) if winner_side == 2 else ""),

        "p1_serve": p1_serve,
        "p2_serve": p2_serve,
        # Raw serving player indicator: "1" or "2", blank when not live
        "player2serve": serve if (is_live and serve in ("1", "2")) else "",

        # Point score within the current game ('00', '15', '30', '40', 'AD') – live only
        "p1_points": str(match.get("game1") or "") if is_live else "",
        "p2_points": str(match.get("game2") or "") if is_live else "",

        # Courtside scoring detail (blank unless the match is scored on /score)
        "serve_number": str(match.get("serve_number") or "") if is_live else "",
        "point_flag": str(match.get("point_flag") or "") if is_live else "",
        "result_note": str(match.get("result_note") or ""),
        # Where this row's data comes from: "manual" (scored on /score), "staged" (pre-loaded), "tennisticker"
        "data_source": "manual" if match.get("manual") else "staged" if match.get("staged")
        else ("tennisticker" if match else ""),
    }
    manual_stats = match.get("manual_stats") or {}
    for side in (1, 2):
        s = manual_stats.get(side) or manual_stats.get(str(side)) or {}
        pct = lambda v: f"{v}%" if v is not None else ""
        row[f"p{side}_aces"] = str(s.get("aces", "")) if s else ""
        row[f"p{side}_double_faults"] = str(s.get("double_faults", "")) if s else ""
        row[f"p{side}_first_serve_pct"] = pct(s.get("first_serve_pct")) if s else ""
        row[f"p{side}_first_serve_won_pct"] = pct(s.get("first_serve_won_pct")) if s else ""
        row[f"p{side}_second_serve_won_pct"] = pct(s.get("second_serve_won_pct")) if s else ""
        row[f"p{side}_winners"] = str(s.get("winners", "")) if s else ""
        row[f"p{side}_unforced_errors"] = str(s.get("unforced_errors", "")) if s else ""
        row[f"p{side}_break_points"] = f"{s['break_points_won']}/{s['break_points']}" if s else ""
        row[f"p{side}_points_won"] = str(s.get("points_won", "")) if s else ""

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
    if is_live and last_nonzero and last_nonzero < sets_to_include:
        p1_last, p2_last = sets_raw[last_nonzero - 1][1:3]
        # The last set with games is finished, so the next set is in play at 0-0 (not "-")
        if is_set_won(p1_last, p2_last) or is_set_won(p2_last, p1_last):
            current_set = last_nonzero + 1
    if is_live and match.get("manual") and match.get("sets_played_count"):
        # Manual scoring always includes the set in play (works for short sets / Fast4 too)
        current_set = min(int(match["sets_played_count"]), sets_to_include)

    p1_sets_won = 0
    p2_sets_won = 0
    summary_parts = []

    for i, (present, p1, p2, tb) in enumerate(sets_raw, start=1):
        # Show a set only if it has games, or it is the current live set (may be 0-0,
        # including a new set the feed hasn't sent yet)
        show = (present and (p1 > 0 or p2 > 0)) or (is_live and i == current_set)

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

    return {court: pick_court_match(ms) for court, ms in by_court.items()}


def result_finished_at(m):
    """When a finished match's result arrived: the feed's last score change, else its row timestamp."""
    mid = str(m.get("matchid") or "")
    return (manager.feed_score_ts.get(mid) if manager else 0) or m.get("timestamp") or 0


def result_on_hold(m):
    """True while a just-finished feed match should stay its court's on-air match."""
    if RESULT_HOLD_MINUTES <= 0 or m.get("manual") or classify_match_status(m) != "COMPLETED":
        return False
    if str(m.get("matchid")) in released_results:
        return False
    return time.time() - result_finished_at(m) < RESULT_HOLD_MINUTES * 60


def pick_court_match(ms):
    """
    The one match a court shows (vMix and overlays): live; then a finished result still
    on hold (manual result awaiting "mark complete", or a feed result within the hold
    time); then staged (pre-loaded from our own data, earliest first); then the feed's
    next planned match; then the most recently completed.
    """
    live = [x for x in ms if classify_match_status(x) == "LIVE"]
    if live:
        return live[0]
    # A manually scored match that has finished stays on air (winner graphics) until marked complete
    awaiting = [x for x in ms if x.get("awaiting_confirmation")]
    if awaiting:
        return awaiting[0]
    # A feed match that has just finished keeps the court for the result hold (scores + winner on air)
    held = [x for x in ms if result_on_hold(x)]
    if held:
        return max(held, key=result_finished_at)
    staged = [x for x in ms if x.get("staged") and classify_match_status(x) == "UPCOMING"]
    if staged:
        return sorted(staged, key=lambda x: (str(x.get("schedule_date") or ""),
                                             str(x.get("schedtime") or "") or "99:99"))[0]
    upcoming = [x for x in ms if classify_match_status(x) == "UPCOMING"]
    if upcoming:
        return sorted(upcoming, key=lambda x: str(x.get("schedtime") or ""))[0]
    completed = [x for x in ms if classify_match_status(x) == "COMPLETED"]
    if completed:
        return sorted(completed, key=lambda x: x.get("timestamp") or 0, reverse=True)[0]
    return ms[0] if ms else None


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
    bios = manager.get_all_player_bios()

    rows = []
    for court in sorted(selected.keys(), key=court_sort_key):
        if court_filter:
            # Token-based match so 'court=1' does not also hit 'Court 11'
            tokens = [t.lower() for t in re.split(r'\W+', court) if t]
            if court_filter not in tokens and court_filter != court.lower():
                continue
        rows.append(vmix_flat_row(selected[court], bios=bios))

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
    bios = manager.get_all_player_bios()

    for idx in range(1, num_slots + 1):
        if idx <= len(court_order):
            flat = vmix_flat_row(selected[court_order[idx - 1]], sets_to_include=VMIX_SETS_WIDE, bios=bios)
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
        bug_style=get_bug_style(),
        bug_court=str(court or (match_data or {}).get("court") or '').strip()
    )


@app.route('/caspar/bug/editor', methods=['GET', 'POST'])
def bug_editor():
    """Bug appearance editor: colours, position and point-box visibility with live preview."""
    message = None
    error = None

    if request.method == 'POST':
        if (request.form.get('form_name') or '') == 'reset':
            if manager:
                manager.save_setting(BUG_STYLE_SETTING_KEY, "")
                message = "Bug style reset to defaults."
            else:
                error = "Database not ready yet - try again shortly."
        else:
            ok, msg = save_bug_style_from_form(request.form)
            if ok:
                message = msg
            else:
                error = msg

    # Optional passthrough so the preview shows a specific match/court
    preview_params = []
    for key in ("matchid", "court"):
        val = (request.args.get(key) or '').strip()
        if val:
            preview_params.append(f"{key}={val}")
    preview_qs = "&".join(preview_params)

    response = make_response(render_template(
        'bug_editor.html',
        style=get_bug_style(),
        preview_qs=preview_qs,
        message=message,
        error=error
    ))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


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
# Per-court graphics page + show/hide control API
# ====================================================================

GRAPHIC_NAMES = ("bug", "lower_third", "winner")
GRAPHIC_ACTIONS = ("show", "hide")
GRAPHICS_DEFAULT = {"bug": True, "lower_third": False, "winner": False}
GRAPHICS_STATE = {}  # court -> {graphic: visible}


def get_graphics_state(court):
    with state_lock:
        return dict(GRAPHICS_STATE.get(court, GRAPHICS_DEFAULT))


@app.route('/api/v1/graphics/<path:court>/<graphic>/<action>', methods=['GET', 'POST'])
def graphics_command(court, graphic, action):
    """
    Show/hide a graphic on a court's graphics page (/caspar/court/).
    GET or POST e.g. /api/v1/graphics/LTA-OC-1/lower_third/show
    Graphics: bug, lower_third, winner. Actions: show, hide.
    """
    court = str(court).strip()
    graphic = graphic.strip().lower()
    action = action.strip().lower()

    if graphic not in GRAPHIC_NAMES:
        return jsonify({"error": f"Unknown graphic '{graphic}'. Use one of: {', '.join(GRAPHIC_NAMES)}."}), 400
    if action not in GRAPHIC_ACTIONS:
        return jsonify({"error": f"Unknown action '{action}'. Use 'show' or 'hide'."}), 400
    if not court:
        return jsonify({"error": "Court is required."}), 400

    with state_lock:
        state = dict(GRAPHICS_STATE.get(court, GRAPHICS_DEFAULT))
        state[graphic] = (action == "show")
        GRAPHICS_STATE[court] = state

    socketio.emit('graphic_command', {
        "court": court,
        "graphic": graphic,
        "action": action,
        "state": state
    }, to=f"court_{court}")

    return jsonify({"status": "success", "court": court, "graphic": graphic,
                    "action": action, "state": state})


@app.route('/api/v1/court/<path:court>/next', methods=['GET', 'POST'])
def court_next_match(court):
    """
    End the result hold on a court so it moves straight on to its next match.
    GET works too, for vMix shortcuts / Stream Deck: /api/v1/court/LTA-OC-1/next
    """
    if manager is None:
        return jsonify({"error": "Cache manager not initialized."}), 503
    court = str(court).strip()
    held = [m for m in manager.get_latest_data() if str(m.get("court") or "").strip() == court and result_on_hold(m)]
    released_results.update(str(m.get("matchid")) for m in held)
    if held:
        manager.broadcast_matches([str(m.get("matchid")) for m in held])
    now_on = resolve_match(court=court)
    return jsonify({"status": "success", "court": court, "released": [str(m.get("matchid")) for m in held],
                    "now_showing": str(now_on.get("matchid")) if now_on else ""})


@app.route('/api/v1/graphics/<path:court>', methods=['GET'])
def graphics_state(court):
    """Current visibility state of all graphics on a court's graphics page."""
    court = str(court).strip()
    return jsonify({"status": "success", "court": court, "state": get_graphics_state(court)})


@app.route('/caspar/court/', methods=['GET'])
def caspar_court_page():
    """
    Single overlay page per court for CasparCG/vMix: score bug, lower-third
    (names + flags) and winner graphic, controlled via /api/v1/graphics/...
    The bug auto-hides and the winner graphic auto-shows when the current
    match gains a winner.
    """
    court = (request.args.get("court") or '').strip()
    debug_mode = request.args.get("debug", "0") == "1"

    match_data = resolve_match(court=court, auto_single_live=True, fallback_any=False) if court else None

    return render_template(
        "caspar_court.html",
        court=court,
        match_data=match_data,
        debug_mode=debug_mode,
        bug_style=get_bug_style(),
        graphics_state=get_graphics_state(court)
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
                snapshot = resolve_match(matchid=matchid)
            if snapshot is None and court:
                snapshot = resolve_match(court=court, auto_single_live=True, fallback_any=True)

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
